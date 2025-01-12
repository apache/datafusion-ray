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


import pyarrow as pa
import asyncio
import ray
import uuid
from collections import deque

from tabulate import tabulate

from datafusion_ray._datafusion_ray_internal import (
    RayContext as RayContextInternal,
    RayDataFrame as RayDataFrameInternal,
    batch_to_ipc as rust_batch_to_ipc,
    ipc_to_batch as rust_ipc_to_batch,
)


class RayDataFrame:
    def __init__(self, ray_internal_df: RayDataFrameInternal):
        self.df = ray_internal_df
        self.coordinator_id = self.df.coordinator_id
        self._stages = None
        self._batches = None

    def stages(self, batch_size=8192):
        # create our coordinator now, which we need to create stages
        if not self._stages:
            self.coord = RayStageCoordinator.options(
                name="RayQueryCoordinator:" + self.coordinator_id,
            ).remote(self.coordinator_id)
            self._stages = self.df.stages(batch_size)
        return self._stages

    def execution_plan(self):
        return self.df.execution_plan()

    def collect(self) -> list[pa.RecordBatch]:
        if not self._batches:
            reader = self.reader()
            self._batches = list(reader)
        return self._batches

    def show(self) -> None:
        table = pa.Table.from_batches(self.collect())
        print(tabulate(table.to_pylist(), headers="keys", tablefmt="outline"))

    def reader(self) -> pa.RecordBatchReader:
        refs = [
            self.coord.new_stage.remote(stage.stage_id, stage.plan_bytes())
            for stage in self.stages()
        ]
        # wait for all stages to be created
        ray.wait(refs, num_returns=len(refs))

        ray.get(self.coord.run_stages.remote())

        print("RayDataFrame: Done executing all stages")

        max_stage_id = max([int(stage.stage_id) for stage in self.stages()])

        exchanger = ray.get(self.coord.get_exchanger.remote())
        schema = ray.get(exchanger.get_schema.remote(max_stage_id))

        ray_iterable = RayIterable(exchanger, max_stage_id, 0)
        reader = pa.RecordBatchReader.from_batches(schema, ray_iterable)

        return reader


class RayContext:
    def __init__(self) -> None:
        self.ctx = RayContextInternal()

    def register_parquet(self, name: str, path: str):
        self.ctx.register_parquet(name, path)

    def execution_plan(self):
        return self.ctx.execution_plan()

    def sql(self, query: str) -> RayDataFrame:
        coordinator_id = str(uuid.uuid4())
        self.ctx.set_coordinator_id(coordinator_id)

        df = self.ctx.sql(query, coordinator_id)
        return RayDataFrame(df)

    def set(self, option: str, value: str) -> None:
        self.ctx.set(option, value)


@ray.remote(num_cpus=0)
class RayStageCoordinator:
    def __init__(self, coordinator_id: str) -> None:
        self.my_id = coordinator_id
        self.stages = {}
        self.exchanger = RayExchanger.remote()

    def get_exchanger(self):
        print(f"Coord: returning exchanger {self.exchanger}")
        return self.exchanger

    def new_stage(self, stage_id: str, plan_bytes: bytes):
        try:
            if stage_id in self.stages:
                print(f"already started stage {stage_id}")
                return self.stages[stage_id]

            print(f"creating new stage {stage_id} from bytes {len(plan_bytes)}")
            stage = RayStage.options(
                name="stage:" + stage_id,
            ).remote(stage_id, plan_bytes, self.my_id, self.exchanger)
            self.stages[stage_id] = stage

        except Exception as e:
            print(
                f"RayQueryCoordinator[{self.my_id}] Unhandled Exception in new stage! {e}"
            )
            raise e

    def run_stages(self):
        try:
            refs = [stage.register_schema.remote() for stage in self.stages.values()]

            # wait for all stages to register their schemas
            ray.wait(refs, num_returns=len(refs))

            print(f"RayQueryCoordinator[{self.my_id}] all schemas registered")

            # now we can tell each stage to start executing and stream its results to
            # the exchanger

            refs = [stage.consume.remote() for stage in self.stages.values()]
            # wait for all stages finish before this method finishes
            # ray.wait(refs, num_returns=len(refs))
        except Exception as e:
            print(
                f"RayQueryCoordinator[{self.my_id}] Unhandled Exception in run stages! {e}"
            )
            raise e


@ray.remote(num_cpus=0)
class RayStage:
    def __init__(
        self, stage_id: str, plan_bytes: bytes, coordinator_id: str, exchanger
    ):

        from datafusion_ray._datafusion_ray_internal import PyStage

        self.stage_id = stage_id
        self.pystage = PyStage(stage_id, plan_bytes, coordinator_id)
        self.exchanger = exchanger

    def register_schema(self):
        schema = self.pystage.schema()
        ray.get(self.exchanger.put_schema.remote(self.stage_id, schema))

    def consume(self):
        try:
            for partition in range(self.pystage.num_output_partitions()):
                print(f"RayStage[{self.stage_id}] consuming partition:{partition}")
                reader = self.pystage.execute(partition)
                for batch in reader:
                    ipc_batch = batch_to_ipc(batch)
                    o_ref = ray.put(ipc_batch)

                    # upload a nested object, list[oref] so that ray does not
                    # materialize it at the destination.  The shuffler only
                    # needs to exchange object refs
                    ray.get(
                        self.exchanger.put.remote(self.stage_id, partition, [o_ref])
                    )
                # signal there are no more batches
                ray.get(self.exchanger.put.remote(self.stage_id, partition, None))
        except Exception as e:
            print(f"RayStage[{self.stage_id}] Unhandled Exception in consume: {e}!")
            raise e


@ray.remote(num_cpus=0)
class RayExchanger:
    def __init__(self):
        self.queues = {}
        self.schemas = {}

    async def put_schema(self, stage_id, schema):
        key = int(stage_id)
        self.schemas[key] = schema

    async def get_schema(self, stage_id):
        key = int(stage_id)
        return self.schemas[key]

    async def put(self, stage_id, output_partition, item):
        key = f"{stage_id}-{output_partition}"
        if key not in self.queues:
            self.queues[key] = asyncio.Queue()

        q = self.queues[key]
        await q.put(item)
        # print(f"RayExchanger got batch for {key}")

    async def get(self, stage_id, output_partition):
        key = f"{stage_id}-{output_partition}"
        if key not in self.queues:
            self.queues[key] = asyncio.Queue()

        q = self.queues[key]

        item = await q.get()
        return item


class StageReader:
    def __init__(self, coordinator_id):
        print(f"Stage reader init getting coordinator {coordinator_id}")
        self.coord = ray.get_actor("RayQueryCoordinator:" + coordinator_id)
        print("Stage reader init got it")
        ref = self.coord.get_exchanger.remote()
        print(f"Stage reader init got exchanger ref {ref}")
        self.exchanger = ray.get(ref)
        print("Stage reader init got exchanger")

    def reader(
        self,
        stage_id: str,
        partition: int,
    ) -> pa.RecordBatchReader:
        try:
            print(f"reader [s:{stage_id} p:{partition}] getting reader")
            schema = ray.get(self.exchanger.get_schema.remote(stage_id))
            ray_iterable = RayIterable(self.exchanger, stage_id, partition)
            reader = pa.RecordBatchReader.from_batches(schema, ray_iterable)
            print(f"reader [s:{stage_id} p:{partition}] got it")
            return reader
        except Exception as e:
            print(f"reader [s:{stage_id} p:{partition}] Unhandled Exception! {e}")
            raise e


class RayIterable:
    def __init__(self, exchanger, stage_id, partition):
        self.exchanger = exchanger
        self.stage_id = stage_id
        self.partition = partition

    def __next__(self):
        obj_ref = self.exchanger.get.remote(self.stage_id, self.partition)
        # print(f"[RayIterable stage:{self.stage_id} p:{self.partition}] got ref")
        message = ray.get(obj_ref)

        if message is None:
            raise StopIteration

        # other wise we know its a list of a single object ref
        ipc_batch = ray.get(message[0])

        # print(f"[RayIterable stage:{self.stage_id} p:{self.partition}] got ipc batch")
        batch = ipc_to_batch(ipc_batch)
        # print(
        #    f"[RayIterable stage:{self.stage_id} p:{self.partition}] converted to batch"
        # )

        return batch

    def __iter__(self):
        return self


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
