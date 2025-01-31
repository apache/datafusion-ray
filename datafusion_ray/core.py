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
import time
import random

from datafusion_ray._datafusion_ray_internal import (
    RayContext as RayContextInternal,
    RayDataFrame as RayDataFrameInternal,
    prettify,
)
import datafusion


class RayDataFrame:
    def __init__(
        self,
        ray_internal_df: RayDataFrameInternal,
        num_partitions,
        batch_size=8192,
        isolate_parititions=False,
        bucket: str | None = None,
        num_exchangers: int = 1,
    ):
        self.df = ray_internal_df
        self.coordinator_id = self.df.coordinator_id
        self._stages = None
        self._batches = None
        self.batch_size = batch_size
        self.isolate_partitions = isolate_parititions
        self.bucket = bucket
        self.num_exchangers = num_exchangers
        self.num_partitions = num_partitions

    def stages(self):
        # create our coordinator now, which we need to create stages
        if not self._stages:
            self._stages = self.df.stages(self.batch_size, self.isolate_partitions)

            self.coord = RayStageCoordinator.options(
                name="RayQueryCoordinator:" + self.coordinator_id,
            ).remote(
                self.coordinator_id,
                len(self._stages),
                self.num_exchangers,
                self.num_partitions,
            )

            ray.get(self.coord.start_up.remote())
            print("ray coord started up")
        return self._stages

    def execution_plan(self):
        return self.df.execution_plan()

    def logical_plan(self):
        return self.df.logical_plan()

    def optimized_logical_plan(self):
        return self.df.optimized_logical_plan()

    def collect(self) -> list[pa.RecordBatch]:
        if not self._batches:
            t1 = time.time()
            self.stages()
            t2 = time.time()
            print(f"creating stages took {t2 -t1}s")

            last_stage = max([stage.stage_id for stage in self._stages])

            ref = self.coord.get_exchanger_addr.remote(last_stage, partition=0)
            self.create_ray_stages()
            t3 = time.time()
            print(f"creating ray stage actors took {t3 -t2}s")
            self.run_stages()
            # now collect the result
            addr = ray.get(ref)
            print(
                "addr = ",
            )

            print("calling df execute")
            reader = self.df.execute({(last_stage, 0): addr})
            print("called df execute, got reader")
            self._batches = list(reader)
            self.coord.all_done.remote()
        return self._batches

    def show(self) -> None:
        batches = self.collect()
        print(prettify(batches))

    def create_ray_stages(self):

        # if we are doing each partition separate (isolate_partitions =True)
        # then the plan generated will include a PartitionIsolator which
        # will take care of that.  Our job is to then launch a stage for each
        # partition.
        #
        # Otherwise, we will just launch each stage once and it will take care
        # care of all input parititions itself.
        print("spawning stages per partition")
        refs = []
        for stage in self.stages():
            num_shadows = stage.num_shadow_partitions()
            num_output_partitions = stage.num_output_partitions()
            if self.isolate_partitions:
                print(
                    f"stage {stage.stage_id} has {num_shadows} shadows, consuming_all_partitions={stage.consume_all_partitions()}"
                )
                if num_shadows:
                    for shadow_partition in range(num_shadows):
                        print(f"starting stage {stage.stage_id}:s{shadow_partition}")
                        refs.append(
                            self.coord.new_stage.remote(
                                stage.stage_id,
                                stage.plan_bytes(),
                                required_output_partitions=list(
                                    range(num_output_partitions)
                                ),
                                shadow_partition=shadow_partition,
                                fraction=1.0 / num_shadows,
                                bucket=self.bucket,
                            )
                        )
                elif stage.consume_all_partitions():
                    # even though we said isolate partitions, this stage needs to be run alone and consume all
                    refs.append(
                        self.coord.new_stage.remote(
                            stage.stage_id,
                            stage.plan_bytes(),
                            required_output_partitions=list(
                                range(num_output_partitions)
                            ),
                            bucket=self.bucket,
                        )
                    )

                else:
                    # we are running a single partition as its own Actor
                    for partition in range(num_output_partitions):
                        refs.append(
                            self.coord.new_stage.remote(
                                stage.stage_id,
                                stage.plan_bytes(),
                                required_output_partitions=[partition],
                                bucket=self.bucket,
                            )
                        )
            else:
                # we are running each stage as its own actor
                refs.append(
                    self.coord.new_stage.remote(
                        stage.stage_id,
                        stage.plan_bytes(),
                        required_output_partitions=list(range(num_output_partitions)),
                        bucket=self.bucket,
                    )
                )

        # wait for all stages to be created
        ray.wait(refs, num_returns=len(refs))

    def run_stage(self, stage_key):
        ray.get(self.coord.run_stage.remote(stage_key))

    def run_stages(self):
        ray.get(self.coord.run_stages.remote())


class RayContext:
    def __init__(
        self,
        batch_size: int = 8192,
        num_exchangers: int = 1,
        isolate_partitions: bool = False,
        bucket: str | None = None,
    ) -> None:
        self.ctx = RayContextInternal(bucket)
        self.batch_size = batch_size
        self.num_exchangers = num_exchangers
        self.isolate_partitions = isolate_partitions
        self.bucket = bucket

        if bucket:
            print("registering s3")
            self.ctx.register_s3(self.bucket)

    def register_parquet(self, name: str, path: str):
        self.ctx.register_parquet(name, path)

    def register_listing_table(self, name: str, path: str, file_extention="parquet"):
        self.ctx.register_listing_table(name, path, file_extention)

    def sql(self, query: str) -> RayDataFrame:
        coordinator_id = str(uuid.uuid4())
        self.ctx.set_coordinator_id(coordinator_id)

        df = self.ctx.sql(query, coordinator_id)
        return RayDataFrame(
            df,
            self.ctx.get_target_partitions(),
            self.batch_size,
            self.isolate_partitions,
            self.bucket,
            self.num_exchangers,
        )

    def set(self, option: str, value: str) -> None:
        self.ctx.set(option, value)


@ray.remote(num_cpus=0)
class RayStageCoordinator:
    def __init__(
        self,
        coordinator_id: str,
        num_stages: int,
        num_exchangers: int,
        num_partitions: int,
    ) -> None:
        self.my_id = coordinator_id
        self.stages = {}
        self.num_stages = num_stages
        self.num_exchangers = num_exchangers
        self.num_partitions = num_partitions
        self.runtime_env = {}

    def start_up(self):
        self.determine_environment()
        print(f"Coordinator starting up {self.num_exchangers} exchangers")

        self.xs = [
            RayExchanger.remote(f"Exchanger #{i}") for i in range(self.num_exchangers)
        ]

        refs = [exchange.start_up.remote() for exchange in self.xs]

        # ensure we've done the necessary initialization before continuing
        ray.wait(refs, num_returns=len(refs))
        print("all exchanges started up")

        # for each possible stage, and partition, assign it to an exchanger
        self.exchange_addrs = {}
        for stage_num in range(self.num_stages):
            for partition_num in range(self.num_partitions):
                exchanger_idx = random.choice(range(self.num_exchangers))
                self.exchange_addrs[(stage_num, partition_num)] = ray.get(
                    self.xs[exchanger_idx].addr.remote()
                )
        print(self.exchange_addrs)

        # don't wait for these
        [exchange.serve.remote() for exchange in self.xs]

    def get_exchanger_addr(self, stage_num: int, partition: int):
        return self.exchange_addrs[(stage_num, partition)]

    def get_exchanger(self, idx: int):
        return self.xs[idx]

    def all_done(self):
        print("calling exchangers all done")
        refs = [exchange.all_done.remote() for exchange in self.xs]
        ray.wait(refs, num_returns=len(refs))
        print("done exchangers all done")

    def determine_environment(self):
        env_keys = "AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_REGION AWS_SESSION_TOKEN".split()
        env = {}
        for key in env_keys:
            if key in os.environ:
                env[key] = os.environ[key]
        self.runtime_env["env_vars"] = env

    def new_stage(
        self,
        stage_id: int,
        plan_bytes: bytes,
        required_output_partitions: list[int],
        shadow_partition=None,
        fraction=1.0,
        bucket: str | None = None,
    ):
        stage_key = f"{stage_id}-{shadow_partition}-{required_output_partitions}"
        try:

            print(f"creating new stage {stage_key} from bytes {len(plan_bytes)}")
            stage = RayStage.options(
                name="stage:" + stage_key,
                runtime_env=self.runtime_env,
            ).remote(
                stage_id,
                plan_bytes,
                self.exchange_addrs,
                required_output_partitions,
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

    def run_stage(self, stage_key):
        print(f"running stage {stage_key}")
        try:
            self.stages[stage_key].execute.remote()
        except Exception as e:
            print(
                f"RayQueryCoordinator[{self.my_id}] Unhandled Exception in run stage! {e}"
            )
            raise e

    def run_stages(self):
        print("running stages")
        try:
            for stage_key, stage in self.stages.items():
                print(f"Scheduling stage {stage_key}")
                stage.execute.remote()

        except Exception as e:
            print(
                f"RayQueryCoordinator[{self.my_id}] Unhandled Exception in run stages! {e}"
            )
            raise e


@ray.remote(num_cpus=0)
class RayStage:
    def __init__(
        self,
        stage_id: str,
        plan_bytes: bytes,
        exchanger_addrs: dict[tuple[int, int], str],
        required_output_partitions: list[int],
        fraction: float,
        shadow_partition=None,
        bucket: str | None = None,
    ):

        from datafusion_ray._datafusion_ray_internal import PyStage

        self.shadow_partition = shadow_partition
        shadow = (
            f", shadowing:{self.shadow_partition}"
            if self.shadow_partition is not None
            else ""
        )

        try:
            self.stage_id = stage_id
            self.pystage = PyStage(
                stage_id,
                plan_bytes,
                exchanger_addrs,
                required_output_partitions,
                shadow_partition,
                bucket,
                fraction,
                sequential=True,
            )
        except Exception as e:
            print(
                f"RayStage[{self.stage_id}{shadow}] Unhandled Exception in init: {e}!"
            )
            raise

    def execute(self):
        shadow = (
            f", shadowing:{self.shadow_partition}"
            if self.shadow_partition is not None
            else ""
        )
        print(f"RayStage[{self.stage_id}{shadow}] commencing execution")
        try:
            self.pystage.execute()
        except Exception as e:
            print(
                f"RayStage[{self.stage_id}{shadow}] Unhandled Exception in execute: {e}!"
            )
            raise e
        return self.stage_id


@ray.remote(num_cpus=0)
class RayExchanger:
    def __init__(self, name: str):
        from datafusion_ray._datafusion_ray_internal import PyExchange

        self.exchange = PyExchange(name, 32000)

    def start_up(self):
        self.exchange.start_up()

    def channel_data(self, stage_id: int, partition_id: int):
        return self.exchange.channel_data(stage_id, partition_id)

    def addr(self):
        return self.exchange.addr()

    async def all_done(self):
        await self.exchange.all_done()

    async def serve(self):
        await self.exchange.serve()
        print("RayExchanger done serving")
