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


from collections import defaultdict
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
        query_id: str,
        batch_size=8192,
        isolate_parititions=False,
        bucket: str | None = None,
    ):
        self.df = ray_internal_df
        self.query_id = query_id
        self._stages = None
        self._batches = None
        self.batch_size = batch_size
        self.isolate_partitions = isolate_parititions
        self.bucket = bucket

    def stages(self):
        # create our coordinator now, which we need to create stages
        if not self._stages:
            self._stages = self.df.stages(self.batch_size, self.isolate_partitions)

            self.coord = RayStageCoordinator.options(
                name="RayQueryCoordinator:" + self.query_id,
            ).remote(
                self.query_id,
            )

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
            print("last stage is", last_stage)

            self.create_ray_stages()
            t3 = time.time()
            print(f"creating ray stage actors took {t3 -t2}s")
            self.run_stages()

            addrs = ray.get(self.coord.get_stage_addrs.remote())
            print("addrs", addrs)

            reader = self.df.read_final_stage(last_stage, addrs[last_stage][0])
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
        refs = []
        for stage in self.stages():
            num_shadows = stage.num_shadow_partitions()
            if self.isolate_partitions and num_shadows:
                print(f"stage {stage.stage_id} has {num_shadows} shadows")
                for shadow in range(num_shadows):
                    refs.append(
                        self.coord.new_stage.remote(
                            stage.stage_id, stage.plan_bytes(), shadow
                        )
                    )
            else:
                # we are running each stage as its own actor
                refs.append(
                    self.coord.new_stage.remote(
                        stage.stage_id,
                        stage.plan_bytes(),
                        shadow_partition=None,
                    )
                )

        # wait for all stages to be created
        ray.wait(refs, num_returns=len(refs))

    def run_stages(self):
        self.coord.serve.remote()


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

    def register_listing_table(self, name: str, path: str, file_extention="parquet"):
        self.ctx.register_listing_table(name, path, file_extention)

    def sql(self, query: str) -> RayDataFrame:
        query_id = str(uuid.uuid4())

        df = self.ctx.sql(query)
        return RayDataFrame(
            df,
            query_id,
            self.batch_size,
            self.isolate_partitions,
            self.bucket,
        )

    def set(self, option: str, value: str) -> None:
        self.ctx.set(option, value)


@ray.remote(num_cpus=0)
class RayStageCoordinator:
    def __init__(
        self,
        query_id: str,
    ) -> None:
        self.query_id = query_id
        self.stages = {}
        self.stage_addrs = defaultdict(list)
        self.stages_started = []
        self.stages_ready = False

    async def all_done(self):
        print("calling stage all done")
        refs = [stage.all_done.remote() for stage in self.stages.values()]
        ray.wait(refs, num_returns=len(refs))
        print("done stage all done")

    async def new_stage(
        self,
        stage_id: int,
        plan_bytes: bytes,
        shadow_partition=None,
    ):
        stage_key = (stage_id, shadow_partition)
        try:

            print(f"creating new stage {stage_key} from bytes {len(plan_bytes)}")
            stage = RayStage.options(
                name=f"Stage: {stage_key}, query_id:{self.query_id}",
            ).remote(
                stage_id,
                plan_bytes,
                shadow_partition,
            )
            self.stages[stage_key] = stage
            self.stages_started.append(stage.start_up.remote())

        except Exception as e:
            print(
                f"RayQueryCoordinator[{self.query_id}] Unhandled Exception in new stage! {e}"
            )
            raise e

    async def wait_for_stages_ready(self):
        while not self.stages_ready:
            await asyncio.sleep(0.1)
            print("waiting for stages to be ready")

    async def ensure_stages_ready(self):
        if not self.stages_ready:
            ray.wait(self.stages_started, num_returns=len(self.stages_started))
            await self.sort_out_addresses()
            self.stages_ready = True
        return self.stages_ready

    async def get_stage_addrs(self) -> dict[int, list[str]]:
        await self.wait_for_stages_ready()
        return self.stage_addrs

    async def sort_out_addresses(self):
        for stage_key, stage in self.stages.items():
            stage_id, shadow_partition = stage_key
            print(f" getting stage addr for {stage_id},{shadow_partition}")
            self.stage_addrs[stage_id].append(await stage.addr.remote())

        print(f"stage_addrs: {self.stage_addrs}")
        # now update all the stages with the addresses of peers such
        # that they can contact their child stages
        refs = []
        for stage_key, stage in self.stages.items():
            refs.append(stage.set_stage_addrs.remote(self.stage_addrs))

        ray.wait(refs, num_returns=len(refs))

    async def serve(self):
        await self.ensure_stages_ready()
        print("running stages")
        try:
            for stage_key, stage in self.stages.items():
                print(f"starting serving of stage {stage_key}")
                stage.serve.remote()

        except Exception as e:
            print(
                f"RayQueryCoordinator[{self.query_id}] Unhandled Exception in run stages! {e}"
            )
            raise e


@ray.remote(num_cpus=0)
class RayStage:
    def __init__(
        self,
        stage_id: str,
        plan_bytes: bytes,
        shadow_partition=None,
    ):

        from datafusion_ray._datafusion_ray_internal import StageService

        self.shadow_partition = shadow_partition
        shadow = (
            f", shadowing:{self.shadow_partition}"
            if self.shadow_partition is not None
            else ""
        )

        try:
            self.stage_id = stage_id
            self.stage_service = StageService(
                stage_id,
                plan_bytes,
                shadow_partition,
            )
        except Exception as e:
            print(
                f"StageService[{self.stage_id}{shadow}] Unhandled Exception in init: {e}!"
            )
            raise

    async def start_up(self):
        self.stage_service.start_up()

    async def all_done(self):
        await self.stage_service.all_done()

    async def addr(self):
        return self.stage_service.addr()

    async def set_stage_addrs(self, stage_addrs: dict[int, list[str]]):
        self.stage_service.set_stage_addrs(stage_addrs)

    async def serve(self):
        await self.stage_service.serve()
        print("StageService done serving")
