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
import os

from tabulate import tabulate

from datafusion_ray._datafusion_ray_internal import (
    RayContext as RayContextInternal,
    RayDataFrame as RayDataFrameInternal,
    batch_to_ipc as rust_batch_to_ipc,
    ipc_to_batch as rust_ipc_to_batch,
    PyExchange,
)
import datafusion


class RayDataFrame:
    def __init__(
        self,
        ray_internal_df: RayDataFrameInternal,
        batch_size=8192,
        isolate_parititions=False,
        bucket: str | None = None,
    ):
        self.df = ray_internal_df
        self.coordinator_id = self.df.coordinator_id
        self._stages = None
        self._batches = None
        self.batch_size = batch_size
        self.isolate_partitions = isolate_parititions
        self.bucket = bucket

    def stages(self):
        # create our coordinator now, which we need to create stages
        if not self._stages:
            self.coord = RayStageCoordinator.options(
                name="RayQueryCoordinator:" + self.coordinator_id,
            ).remote(self.coordinator_id)

            print("getting exchanger")
            exchanger = ray.get(self.coord.get_exchanger.remote())
            print("calling serve")
            exchanger.serve.remote()

            self._stages = self.df.stages(self.batch_size, self.isolate_partitions)
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

    def create_stages(self):

        # if we are doing each partition separate (isolate_partitions =True)
        # then the plan generated will include a PartitionIsolator which
        # will take care of that.  Our job is to then launch a stage for each
        # partition.
        #
        # Otherwise, we will just launch each stage once and it will take care
        # care of all input parititions itself.
        #
        if self.isolate_partitions:
            print("spawning stages per partition")
            refs = []
            for stage in self.stages():
                num_shadows = stage.num_shadow_partitions()
                print(f"stage {stage.stage_id} has {num_shadows} shadows")
                if num_shadows:
                    for shadow_partition in range(num_shadows):
                        print(f"starting stage {stage.stage_id}:s{shadow_partition}")
                        refs.append(
                            self.coord.new_stage.remote(
                                stage.stage_id,
                                stage.plan_bytes(),
                                shadow_partition,
                                1.0 / num_shadows,
                                self.bucket,
                            )
                        )
                else:
                    refs.append(
                        self.coord.new_stage.remote(
                            stage.stage_id, stage.plan_bytes(), bucket=self.bucket
                        )
                    )
        else:
            print("creating stages")
            refs = [
                self.coord.new_stage.remote(
                    stage.stage_id, stage.plan_bytes(), bucket=self.bucket
                )
                for stage in self.stages()
            ]
        # wait for all stages to be created

        ray.wait(refs, num_returns=len(refs))

    def run_stages(self):

        ray.get(self.coord.run_stages.remote())
        return

        max_stage_id = max([int(stage.stage_id) for stage in self.stages()])

        exchanger = ray.get(self.coord.get_exchanger.remote())
        schema = ray.get(exchanger.get_schema.remote(max_stage_id))

        ray_iterable = RayIterable(exchanger, max_stage_id, 0, self.coordinator_id)
        reader = pa.RecordBatchReader.from_batches(schema, ray_iterable)

        return reader

    def totals(self):
        return ray.get(self.coord.stats.remote())


class RayContext:
    def __init__(
        self,
        batch_size: int = 8192,
        isolate_partitions: bool = False,
        bucket: str | None = None,
    ) -> None:
        self.ctx = RayContextInternal(bucket)
        self.batch_size = batch_size
        self.isolate_partitions = isolate_partitions
        self.bucket = bucket

        if bucket:
            print("registering s3")
            self.ctx.register_s3(self.bucket)

    def register_parquet(self, name: str, path: str):
        self.ctx.register_parquet(name, path)

    def execution_plan(self):
        return self.ctx.execution_plan()

    def sql(self, query: str) -> RayDataFrame:
        coordinator_id = str(uuid.uuid4())
        self.ctx.set_coordinator_id(coordinator_id)

        df = self.ctx.sql(query, coordinator_id)
        return RayDataFrame(df, self.batch_size, self.isolate_partitions, self.bucket)

    def local_sql(self, query: str) -> datafusion.DataFrame:
        coordinator_id = str(uuid.uuid4())
        self.ctx.set_coordinator_id(coordinator_id)
        return self.ctx.local_sql(query)

    def set(self, option: str, value: str) -> None:
        self.ctx.set(option, value)


@ray.remote(num_cpus=0)
class RayStageCoordinator:
    def __init__(self, coordinator_id: str) -> None:
        self.my_id = coordinator_id
        self.stages = {}
        self.exchanger = RayExchanger.remote()
        self.totals = {}
        self.runtime_env = {}
        self.determine_environment()

    def get_exchanger(self):
        return self.exchanger

    def determine_environment(self):
        env_keys = "AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_REGION AWS_SESSION_TOKEN".split()
        env = {}
        for key in env_keys:
            if key in os.environ:
                env[key] = os.environ[key]
        self.runtime_env["env_vars"] = env

    def new_stage(
        self,
        stage_id: str,
        plan_bytes: bytes,
        shadow_partition=None,
        fraction=1.0,
        bucket: str | None = None,
    ):
        stage_key = f"{stage_id}-{shadow_partition}"
        print(f"creating new stage {stage_key} from bytes {len(plan_bytes)}")
        try:
            exchange_addr = ray.get(self.exchanger.addr.remote())
            print("addr is ", exchange_addr)
            if stage_key in self.stages:
                print(f"already started stage {stage_key}")
                return self.stages[stage_key]

            print(f"creating new stage {stage_key} from bytes {len(plan_bytes)}")
            stage = RayStage.options(
                name="stage:" + stage_key,
                runtime_env=self.runtime_env,
            ).remote(
                stage_id,
                plan_bytes,
                exchange_addr,
                fraction,
                shadow_partition,
                bucket,
            )
            self.stages[stage_key] = stage

        except Exception as e:
            print(
                f"RayQueryCoordinator[{self.my_id}] Unhandled Exception in new stage! {e}"
            )
            raise e

    def run_stages(self):
        print("running stages")
        try:
            # refs = [stage.consume.remote() for stage in self.stages.values()]
            list(self.stages.values())[0].execute.remote()
        except Exception as e:
            print(
                f"RayQueryCoordinator[{self.my_id}] Unhandled Exception in run stages! {e}"
            )
            raise e

    def report_totals(self, stage_id, partition, total, read_write):
        if stage_id not in self.totals:
            self.totals[stage_id] = {}
        if read_write not in self.totals[stage_id]:
            self.totals[stage_id][read_write] = {}
        if "sum" not in self.totals[stage_id][read_write]:
            self.totals[stage_id][read_write]["sum"] = 0
        if "partition" not in self.totals[stage_id][read_write]:
            self.totals[stage_id][read_write]["partition"] = {}

        self.totals[stage_id][read_write]["partition"][partition] = total
        self.totals[stage_id][read_write]["sum"] += total

    def stats(self):
        return self.totals


@ray.remote(num_cpus=0)
class RayStage:
    def __init__(
        self,
        stage_id: str,
        plan_bytes: bytes,
        exchanger_addr: str,
        fraction: float,
        shadow_partition=None,
        bucket: str | None = None,
    ):

        from datafusion_ray._datafusion_ray_internal import PyStage

        self.stage_id = stage_id
        self.pystage = PyStage(
            stage_id, plan_bytes, exchanger_addr, shadow_partition, bucket, fraction
        )
        self.shadow_partition = shadow_partition

    def execute(self):
        shadow = (
            f", shadowing:{self.shadow_partition}"
            if self.shadow_partition is not None
            else ""
        )

        try:
            for partition in range(self.pystage.num_output_partitions()):
                print(
                    f"RayStage[{self.stage_id}{shadow}] consuming partition:{partition}"
                )
                self.pystage.execute(partition)
        except Exception as e:
            print(f"RayStage[{self.stage_id}] Unhandled Exception in execute: {e}!")
            raise e

    def consume(self):
        shadow = (
            f", shadowing:{self.shadow_partition}"
            if self.shadow_partition is not None
            else ""
        )
        try:
            for partition in range(self.pystage.num_output_partitions()):
                print(
                    f"RayStage[{self.stage_id}{shadow}] consuming partition:{partition}"
                )
                total_rows = 0
                reader = self.pystage.execute(partition)

                pending_refs = []

                for batch in reader:
                    total_rows += len(batch)
                    ipc_batch = batch_to_ipc(batch)
                    o_ref = ray.put(ipc_batch)

                    # print(
                    #    f"RayStage[{self.stage_id}{shadow}] produced batch:{print_batch(batch)}"
                    # )

                    # upload a nested object, list[oref] so that ray does not
                    # materialize it at the destination.  The shuffler only
                    # needs to exchange object refs
                    ref = self.exchanger.put.remote(self.stage_id, partition, [o_ref])
                    pending_refs.append(ref)

                    if len(pending_refs) >= self.max_in_flight_puts:
                        _, pending_refs = ray.wait(pending_refs, num_returns=1)

                # before we send the done signal, let our puts finish
                ray.wait(pending_refs, num_returns=len(pending_refs))

                # signal there are no more batches
                ray.get(
                    self.exchanger.done.remote(self.stage_id, partition, self.fraction)
                )

                self.coord.report_totals.remote(
                    self.stage_id,
                    f"{partition}-{self.shadow_partition}",
                    total_rows,
                    "write",
                )
        except Exception as e:
            print(
                f"RayStage[{self.stage_id}{shadow}] Unhandled Exception in consume: {e}!"
            )
            raise e


@ray.remote(num_cpus=0)
class RayExchanger:
    def __init__(self):
        from datafusion_ray import PyExchange

        self.exchange = PyExchange()

    def addr(self):
        return self.exchange.addr()

    def serve(self):
        self.exchange.serve()


class StageReader:
    def __init__(self, coordinator_id):
        print(f"Stage reader init getting coordinator {coordinator_id}")
        self.coord = ray.get_actor("RayQueryCoordinator:" + coordinator_id)
        print("Stage reader init got it")
        ref = self.coord.get_exchanger.remote()
        print(f"Stage reader init got exchanger ref {ref}")
        self.exchanger = ray.get(ref)
        print("Stage reader init got exchanger")
        self.coordinator_id = coordinator_id

    def reader(
        self,
        stage_id: str,
        partition: int,
    ) -> pa.RecordBatchReader:
        try:
            print(f"reader [s:{stage_id} p:{partition}] getting reader")
            schema = ray.get(self.exchanger.get_schema.remote(stage_id))
            ray_iterable = RayIterable(
                self.exchanger, stage_id, partition, self.coordinator_id
            )
            reader = pa.RecordBatchReader.from_batches(schema, ray_iterable)
            print(f"reader [s:{stage_id} p:{partition}] got it")
            return reader
        except Exception as e:
            print(f"reader [s:{stage_id} p:{partition}] Unhandled Exception! {e}")
            raise e


class RayIterable:
    def __init__(self, exchanger, stage_id, partition, coordinator_id):
        self.exchanger = exchanger
        self.stage_id = stage_id
        self.partition = partition
        self.total_rows = 0
        self.coord = ray.get_actor("RayQueryCoordinator:" + coordinator_id)

    def __next__(self):
        obj_ref = self.exchanger.get.remote(self.stage_id, self.partition)
        # print(f"[RayIterable stage:{self.stage_id} p:{self.partition}] got ref")
        message = ray.get(obj_ref)

        if message is None:
            raise StopIteration

        # other wise we know its a list of a single object ref
        ipc_batch = ray.get(message[0])

        batch = ipc_to_batch(ipc_batch)
        self.total_rows += len(batch)
        # print(
        #    f"[RayIterable stage:{self.stage_id} p:{self.partition}] got batch:\n{print_batch(batch)}"
        # )

        return batch

    def __iter__(self):
        return self

    def __del__(self):
        self.coord.report_totals.remote(
            self.stage_id, self.partition, self.total_rows, "read"
        )


def print_batch(batch) -> str:
    return tabulate(batch.to_pylist(), headers="keys", tablefmt="simple_grid")


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
