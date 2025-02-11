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
import logging
import os
import pyarrow as pa
import asyncio
import ray
import uuid
import time

from datafusion_ray._datafusion_ray_internal import (
    RayContext as RayContextInternal,
    RayDataFrame as RayDataFrameInternal,
    prettify,
)


def setup_logging():
    import logging

    logging.addLevelName(5, "TRACE")

    log_level = os.environ.get("DATAFUSION_RAY_LOG_LEVEL", "WARN").upper()

    # this logger gets captured and routed to rust.   See src/lib.rs
    logging.getLogger("core_py").setLevel(log_level)
    logging.basicConfig()


setup_logging()

_log_level = os.environ.get("DATAFUSION_RAY_LOG_LEVEL", "ERROR").upper()
_rust_backtrace = os.environ.get("RUST_BACKTRACE", "0")
runtime_env = {
    "worker_process_setup_hook": setup_logging,
    "env_vars": {
        "DATAFUSION_RAY_LOG_LEVEL": _log_level,
        "RAY_worker_niceness": "0",
        "RUST_BACKTRACE": _rust_backtrace,
    },
}

log = logging.getLogger("core_py")


def call_sync(coro):
    """call a coroutine in the current event loop or run a new one, and synchronously
    return the result"""
    try:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(coro)
        else:
            return loop.run_until_complete(coro)
    except Exception as e:
        log.error(f"Error in call: {e}")
        log.exception(e)


async def wait_for(coros, name=""):
    return_values = []
    done, _ = await asyncio.wait(coros)
    for d in done:
        e = d.exception()
        if e is not None:
            log.error(f"Exception waiting {name}: {e}")
        else:
            return_values.append(d.result())
    return return_values


class RayDataFrame:
    def __init__(
        self,
        ray_internal_df: RayDataFrameInternal,
        query_id: str,
        batch_size=8192,
        partitions_per_worker: int | None = None,
        prefetch_buffer_size=0,
    ):
        self.df = ray_internal_df
        self.query_id = query_id
        self._stages = None
        self._batches = None
        self.batch_size = batch_size
        self.partitions_per_worker = partitions_per_worker
        self.prefetch_buffer_size = prefetch_buffer_size

    def stages(self):
        # create our coordinator now, which we need to create stages
        if not self._stages:
            self._stages = self.df.stages(
                self.batch_size, self.prefetch_buffer_size, self.partitions_per_worker
            )

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
            log.debug(f"creating stages took {t2 -t1}s")

            last_stage = max([stage.stage_id for stage in self._stages])
            log.debug(f"last stage is {last_stage}")

            self.create_ray_stages()
            t3 = time.time()
            log.debug(f"creating ray stage actors took {t3 -t2}s")
            self.run_stages()

            addrs = ray.get(self.coord.get_stage_addrs.remote())

            reader = self.df.read_final_stage(last_stage, addrs[last_stage][0][0])
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
            for partition_group in stage.partition_groups:
                refs.append(
                    self.coord.new_stage.remote(
                        stage.stage_id,
                        stage.plan_bytes(),
                        partition_group,
                        stage.num_output_partitions,
                        stage.full_partitions,
                    )
                )

        # wait for all stages to be created
        # ray.wait(refs, num_returns=len(refs))
        call_sync(wait_for(refs, "creating ray stages"))

    def run_stages(self):
        self.coord.serve.remote()


class RayContext:
    def __init__(
        self,
        batch_size: int = 8192,
        prefetch_buffer_size: int = 0,
        partitions_per_worker: int | None = None,
    ) -> None:
        self.ctx = RayContextInternal()
        self.batch_size = batch_size
        self.partitions_per_worker = partitions_per_worker
        self.prefetch_buffer_size = prefetch_buffer_size

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
            self.partitions_per_worker,
            self.prefetch_buffer_size,
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
        self.stage_addrs = defaultdict(lambda: defaultdict(list))
        self.output_partitions = {}
        self.stages_started = []
        self.stages_ready = asyncio.Event()

    async def all_done(self):
        log.debug("calling stage all done")
        refs = [stage.all_done.remote() for stage in self.stages.values()]
        # ray.wait(refs, num_returns=len(refs))
        await wait_for(refs, "stages to be all done")
        log.debug("done stage all done")

    async def new_stage(
        self,
        stage_id: int,
        plan_bytes: bytes,
        partition_group: list[int],
        num_output_partitions: int,
        full_partitions: bool,
    ):

        try:
            if stage_id in self.output_partitions:
                assert self.output_partitions[stage_id] == num_output_partitions
            else:
                self.output_partitions[stage_id] = num_output_partitions

            # we need a tuple so its hashable
            partition_set = tuple(partition_group)
            stage_key = (stage_id, partition_set, full_partitions)

            log.debug(f"creating new stage {stage_key} from bytes {len(plan_bytes)}")
            stage = RayStage.options(
                name=f"Stage: {stage_key}, query_id:{self.query_id}",
            ).remote(stage_id, plan_bytes, partition_group)
            self.stages[stage_key] = stage
            self.stages_started.append(stage.start_up.remote())

        except Exception as e:
            log.error(
                f"RayQueryCoordinator[{self.query_id}] Unhandled Exception in new stage! {e}"
            )
            raise e

    async def wait_for_stages_ready(self):
        log.debug("waiting for stages to be ready")
        await self.stages_ready.wait()

    async def ensure_stages_ready(self):
        # ray.wait(self.stages_started, num_returns=len(self.stages_started))
        log.debug(f"going to wait for {self.stages_started}")
        await wait_for(self.stages_started, "stages to be started")
        await self.sort_out_addresses()
        log.info("all stages ready")
        self.stages_ready.set()

    async def get_stage_addrs(self) -> dict[int, list[str]]:
        log.debug("Checking to ensure stages are ready before returning addrs")
        await self.wait_for_stages_ready()
        log.debug("Looks like they are ready")
        return self.stage_addrs

    async def sort_out_addresses(self):
        """Iterate through our stages and gather all of their listening addresses.
        Then, provide the addresses to of peer stages to each stage.
        """

        # first go get all addresses from the stages we launched, concurrently
        # pipeline this by firing up all tasks before awaiting any results
        addrs_by_stage = defaultdict(list)
        addrs_by_stage_partition = defaultdict(dict)
        for stage_key, stage in self.stages.items():
            stage_id, partition_set, full_partitions = stage_key
            a_future = stage.addr.remote()
            addrs_by_stage[stage_id].append(a_future)
            for partition in partition_set:
                addrs_by_stage_partition[stage_id][partition] = a_future

        for stage_key, stage in self.stages.items():
            stage_id, partition_set, full_partitions = stage_key
            if full_partitions:
                for partition in range(self.output_partitions[stage_id]):
                    self.stage_addrs[stage_id][partition] = await wait_for(
                        [addrs_by_stage_partition[stage_id][partition]]
                    )
            else:
                for partition in range(self.output_partitions[stage_id]):
                    self.stage_addrs[stage_id][partition] = await wait_for(
                        addrs_by_stage[stage_id]
                    )

        if log.level <= logging.DEBUG:
            out = ""
            for stage_id, partition_addrs in self.stage_addrs.items():
                out += f"Stage {stage_id}: \n"
                for partition, addrs in partition_addrs.items():
                    out += f"  partition {partition}: {addrs}\n"
            log.debug(f"stage_addrs:\n{out}")
        # now update all the stages with the addresses of peers such
        # that they can contact their child stages
        refs = []
        for stage_key, stage in self.stages.items():
            refs.append(stage.set_stage_addrs.remote(self.stage_addrs))

        # ray.wait(refs, num_returns=len(refs))
        await wait_for(refs, "stages to to have addrs set")
        log.debug("all stage addrs set? or should be")

    async def serve(self):
        await self.ensure_stages_ready()
        log.info("running stages")
        try:
            for stage_key, stage in self.stages.items():
                log.info(f"starting serving of stage {stage_key}")
                stage.serve.remote()

        except Exception as e:
            log.error(
                f"RayQueryCoordinator[{self.query_id}] Unhandled Exception in run stages! {e}"
            )
            raise e


@ray.remote(num_cpus=0)
class RayStage:
    def __init__(
        self,
        stage_id: int,
        plan_bytes: bytes,
        partition_group: list[int],
    ):

        from datafusion_ray._datafusion_ray_internal import StageService

        try:
            self.stage_id = stage_id
            self.stage_service = StageService(
                stage_id,
                plan_bytes,
                partition_group,
            )
        except Exception as e:
            log.error(
                f"StageService[{self.stage_id}{partition_group}] Unhandled Exception in init: {e}!"
            )
            raise

    async def start_up(self):
        # this method is sync
        self.stage_service.start_up()

    async def all_done(self):
        await self.stage_service.all_done()

    async def addr(self):
        return self.stage_service.addr()

    async def set_stage_addrs(self, stage_addrs: dict[int, list[str]]):
        await self.stage_service.set_stage_addrs(stage_addrs)

    async def serve(self):
        await self.stage_service.serve()
        log.info("StageService done serving")
