# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import threading

import datafusion
import pyarrow as pa
import ray
import uuid
import time
from collections import deque

from tabulate import tabulate

from datafusion_ray._datafusion_ray_internal import (
    RayContext as RayContextInternal,
    PlanExecutor,
    batch_to_ipc as rust_batch_to_ipc,
    ipc_to_batch as rust_ipc_to_batch,
)


class RayDataFrame:
    def __init__(self, coordinator_id, plan_bytes):
        self.coordinator_id = coordinator_id
        self.plan_bytes = plan_bytes
        self.batches = None

    def collect(self) -> list[pa.RecordBatch]:
        if not self.batches:
            reader = self.reader()
            self.batches = list(reader)
        return self.batches

    def show(self) -> None:
        table = pa.Table.from_batches(self.collect())
        print(tabulate(table.to_pylist(), headers="keys", tablefmt="outline"))

    def local_reader(self) -> pa.RecordBatchReader:
        pe = PlanExecutor("RootStage", self.plan_bytes, self.coordinator_id)
        return pe.execute(0)

    def reader(self) -> pa.RecordBatchReader:
        self.actor = RayQueryCoordinator.options(
            name="RayQueryCoordinator:" + self.coordinator_id,
            # lifetime="detached",
        ).remote(self.coordinator_id)

        stage = self.actor.new_stage.remote("RootStage", self.plan_bytes)
        stage = ray.get(stage)

        iterator = stage.batches.remote(0)

        return RayIterable("RayIterable [Root] partition:0", iterator)


class RayContext:
    def __init__(self) -> None:
        self.ctx = RayContextInternal()

    def register_parquet(self, name: str, path: str):
        self.ctx.register_parquet(name, path)

    def sql(self, query: str) -> RayDataFrame:
        coordinator_id = str(uuid.uuid4())
        self.ctx.set_coordinator_id(coordinator_id)

        plan_bytes = self.ctx.sql_to_physical_plan_bytes(query)
        return RayDataFrame(coordinator_id, plan_bytes)

    def basic(self, query: str) -> RayDataFrame:
        coordinator_id = str(uuid.uuid4())
        self.ctx.set_coordinator_id(coordinator_id)

        plan_bytes = self.ctx.basic_physical_plan_bytes(query)
        return RayDataFrame(coordinator_id, plan_bytes)

    def two_step(self, query: str) -> RayDataFrame:
        coordinator_id = str(uuid.uuid4())
        self.ctx.set_coordinator_id(coordinator_id)

        plan_bytes = self.ctx.two_step_physical_plan_bytes(query)
        return RayDataFrame(coordinator_id, plan_bytes)

    def set(self, option: str, value: str) -> None:
        self.ctx.set(option, value)


class RayIterable:
    def __init__(self, name, iterable):
        self.name = name
        self.iterable = iterable

    def __next__(self):
        obj_ref = next(self.iterable)
        print(f"[{self.name}] got ref")
        ipc_batch = ray.get(obj_ref)
        print(f"[{self.name}] got ipc batch")
        batch = ipc_to_batch(ipc_batch)
        print(f"[{self.name}] converted to batch")

        return batch

    def __iter__(self):
        return self


@ray.remote(num_cpus=0)
class RayQueryCoordinator:
    def __init__(self, coordinator_id: str) -> None:
        self.my_id = coordinator_id
        self.stages = {}

    def new_stage(self, stage_id, plan):
        try:
            if stage_id in self.stages:
                print(f"already started stage {stage_id}")
                return self.stages[stage_id]

            stage = Stage.options(
                name="stage:" + stage_id,
                # lifetime="detached",
            ).remote(stage_id, plan, self.my_id)
            self.stages[stage_id] = stage

            return stage
        except Exception as e:
            print(f"RayQueryCoordinator[{self.my_id}] Unhandled Exception! {e}")
            raise e


def execute_stage(
    plan: bytes,
    partition: int,
    stage_id: str,
    coordinator_id: str,
) -> pa.RecordBatchReader:
    try:
        print(f"execute_stage [{stage_id}] executing partition {partition}")
        actor = ray.get_actor("RayQueryCoordinator:" + coordinator_id)
        stage = actor.new_stage.remote(stage_id, plan)
        stage = ray.get(stage)

        iterable = stage.batches.remote(partition)
        print(f"execute_stage [{stage_id}] made iterable")

        schema = ray.get(stage.schema.remote())

        ray_iterable = RayIterable(
            f"RayIterable [{stage_id}] partition:{partition} ", iterable
        )
        reader = pa.RecordBatchReader.from_batches(schema, ray_iterable)
        return reader
    except Exception as e:
        print(f"execute_stage [{stage_id}] Unhandled Exception! {e}")
        raise e


@ray.remote(num_cpus=0)
class Stage:
    def __init__(self, name: str, plan: bytes, coordinator_id: str):

        from datafusion_ray._datafusion_ray_internal import PlanExecutor

        self.name = name
        self.executor = PlanExecutor(name, plan, coordinator_id)

        try:
            self.readers = [
                self.executor.execute(p)
                for p in range(self.executor.num_output_partitions())
            ]
        except Exception as e:
            print(f"Stage[{self.name}] Unhandled Exception in init {e}!")
            raise e

    def batches(self, partition):
        try:
            start = time.time()
            batch = next(self.readers[partition])
            end = time.time()
            print(
                f"Stage[{self.name}] got batch ({len(batch)} rows) in {end - start} s size of batch {batch.get_total_buffer_size()}"
            )
            ipc_batch = batch_to_ipc(batch)
            print(
                f"Stage[{self.name}] got ipc_batch size {len(ipc_batch)} type {type(ipc_batch)}"
            )
            yield ipc_batch
        except StopIteration:
            return
        except Exception as e:
            print(f"Stage[{self.name}] Unhandled Exception in batches {e}!")
            raise e

    def schema(self):
        return self.executor.schema()


def batch_to_ipc(batch: pa.RecordBatch) -> bytes:
    # sink = pa.BufferOutputStream()
    # with pa.ipc.new_stream(sink, batch.schema) as writer:
    #    writer.write_batch(batch)

    # work around for non alignment issue for FFI buffers
    # return sink.getvalue()
    return rust_batch_to_ipc(batch)


def ipc_to_batch(data) -> pa.RecordBatch:
    # with pa.ipc.open_stream(data) as reader:
    #    return reader.read_next_batch()
    return rust_ipc_to_batch(data)
