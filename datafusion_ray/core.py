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
from dataclasses import dataclass
import logging
import os
import pyarrow as pa
import asyncio
import ray
import json
import time

from .friendly import new_friendly_name

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
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    else:
        return loop.run_until_complete(coro)


# work around for https://github.com/ray-project/ray/issues/31606
async def _ensure_coro(maybe_obj_ref):
    return await maybe_obj_ref


async def wait_for(coros, name=""):
    """Wait for all coros to complete and return their results.
    Does not preserve ordering."""

    return_values = []
    # wrap the coro in a task to work with python 3.10 and 3.11+ where asyncio.wait semantics
    # changed to not accept any awaitable
    start = time.time()
    done, _ = await asyncio.wait([asyncio.create_task(_ensure_coro(c)) for c in coros])
    end = time.time()
    log.info(f"waiting for {name} took {end - start}s")
    for d in done:
        e = d.exception()
        if e is not None:
            log.error(f"Exception waiting {name}: {e}")
            raise e
        else:
            return_values.append(d.result())
    return return_values


class RayStagePool:
    """A pool of RayStage actors that can be acquired and released"""

    # TODO: We can probably manage this set in a better way
    # This is not a threadsafe implementation.
    # This is simple though and will suffice for now

    def __init__(self, min_workers: int, max_workers: int):
        self.min_workers = min_workers
        self.max_workers = max_workers

        # a map of stage_key (a random identifier) to stage actor reference
        self.pool = {}
        # a map of stage_key to listening address
        self.addrs = {}

        # holds object references from the start_up method for each stage
        # we know all stages are listening when all of these refs have
        # been waited on.  When they are ready we remove them from this set
        self.stages_started = set()

        # an event that is set when all stages are ready to serve
        self.stages_ready = asyncio.Event()

        # stages that are started but we need to get their address
        self.need_address = set()

        # stages that we have the address for but need to start serving
        self.need_serving = set()

        # stages in use
        self.acquired = set()

        # stages available
        self.available = set()

        for _ in range(min_workers):
            self._new_stage()

        log.info(
            f"created ray stage pool (min_workers: {min_workers}, max_workers: {max_workers})"
        )

    async def start(self):
        if not self.stages_ready.is_set():
            await self._wait_for_stages_started()
            await self._wait_for_get_addrs()
            await self._wait_for_serve()
            self.stages_ready.set()

    async def wait_for_ready(self):
        await self.stages_ready.wait()

    async def acquire(self, need=1):
        stage_keys = []

        have = len(self.available)
        total = len(self.available) + len(self.acquired)
        can_make = self.max_workers - total

        need_to_make = need - have

        if need_to_make > can_make:
            raise Exception(f"Cannot allocate workers above {self.max_workers}")

        if need_to_make > 0:
            log.debug(f"creating {need_to_make} additional stages")
            for _ in range(need_to_make):
                self._new_stage()
            await wait_for([self.start()], "waiting for created stages")

        assert len(self.available) >= need

        for _ in range(need):
            stage_key = self.available.pop()
            self.acquired.add(stage_key)

            stage_keys.append(stage_key)

        stages = [self.pool[sk] for sk in stage_keys]
        addrs = [self.addrs[sk] for sk in stage_keys]
        return (stages, stage_keys, addrs)

    def release(self, stage_keys: list[str]):
        for stage_key in stage_keys:
            self.acquired.remove(stage_key)
            self.available.add(stage_key)

    def _new_stage(self):
        self.stages_ready.clear()
        stage_key = new_friendly_name()
        log.debug(f"starting stage: {stage_key}")
        stage = RayStage.options(name=f"Stage: {stage_key}").remote(stage_key)
        self.pool[stage_key] = stage
        self.stages_started.add(stage.start_up.remote())
        self.available.add(stage_key)

    async def _wait_for_stages_started(self):
        log.info("waiting for stages to be ready")
        started_keys = await wait_for(self.stages_started, "stages to be started")
        # we need the addresses of these stages still
        self.need_address.update(set(started_keys))
        # we've started all the stages we know about
        self.stages_started = set()
        log.info("stages are all listening")

    async def _wait_for_get_addrs(self):
        # get the addresses in a pipelined fashion
        refs = []
        stage_keys = []
        for stage_key in self.need_address:
            stage = self.pool[stage_key]
            refs.append(stage.addr.remote())
            stage_keys.append(stage_key)

            self.need_serving.add(stage_key)

        addrs = await wait_for(refs, "stage addresses")

        for key, addr in addrs:
            self.addrs[key] = addr

        self.need_address = set()

    async def _wait_for_serve(self):
        log.info("running stages")
        try:
            for stage_key in self.need_serving:
                log.info(f"starting serving of stage {stage_key}")
                stage = self.pool[stage_key]
                stage.serve.remote()
            self.need_serving = set()

        except Exception as e:
            log.error(f"StagePool: Uhandled Exception in serve: {e}")
            raise e

    async def all_done(self):
        log.info("calling stage all done")
        refs = [stage.all_done.remote() for stage in self.pool.values()]
        await wait_for(refs, "stages to be all done")
        log.info("all stages shutdown")


@ray.remote(num_cpus=0)
class RayStage:
    def __init__(self, stage_key):
        self.stage_key = stage_key

        # import this here so ray doesn't try to serialize the rust extension
        from datafusion_ray._datafusion_ray_internal import StageService

        self.stage_service = StageService(stage_key)

    async def start_up(self):
        # this method is sync
        self.stage_service.start_up()
        return self.stage_key

    async def all_done(self):
        await self.stage_service.all_done()

    async def addr(self):
        return (self.stage_key, self.stage_service.addr())

    async def update_plan(
        self,
        stage_id: int,
        stage_addrs: dict[int, dict[int, list[str]]],
        partition_group: list[int],
        plan_bytes: bytes,
    ):
        await self.stage_service.update_plan(
            stage_id,
            stage_addrs,
            partition_group,
            plan_bytes,
        )

    async def serve(self):
        log.info(f"[{self.stage_key}] serving on {self.stage_service.addr()}")
        await self.stage_service.serve()
        log.info(f"[{self.stage_key}] done serving")


@dataclass
class StageData:
    stage_id: int
    plan_bytes: bytes
    partition_group: list[int]
    child_stage_ids: list[int]
    num_output_partitions: int
    full_partitions: bool


@dataclass
class InternalStageData:
    stage_id: int
    plan_bytes: bytes
    partition_group: list[int]
    child_stage_ids: list[int]
    num_output_partitions: int
    full_partitions: bool
    remote_stage: ...  # ray.actor.ActorHandle[RayStage]
    remote_addr: str

    def __str__(self):
        return f"""Stage: {self.stage_id}, pg: {self.partition_group}, child_stages:{self.child_stage_ids}, listening addr:{self.remote_addr}"""


@ray.remote(num_cpus=0)
class RayContextSupervisor:
    def __init__(
        self,
        worker_pool_min: int,
        worker_pool_max: int,
    ) -> None:
        log.info(f"Creating RayContextSupervisor worker_pool_min: {worker_pool_min}")
        self.pool = RayStagePool(worker_pool_min, worker_pool_max)
        self.stages: dict[str, InternalStageData] = {}
        log.info("Created RayContextSupervisor")

    async def start(self):
        await self.pool.start()

    async def wait_for_ready(self):
        await self.pool.wait_for_ready()

    async def get_stage_addrs(self, stage_id: int):
        addrs = [
            sd.remote_addr for sd in self.stages.values() if sd.stage_id == stage_id
        ]
        return addrs

    async def new_query(
        self,
        stage_datas: list[StageData],
    ):
        if len(self.stages) > 0:
            self.pool.release(list(self.stages.keys()))

        remote_stages, remote_stage_keys, remote_addrs = await self.pool.acquire(
            len(stage_datas)
        )
        self.stages = {}

        for i, sd in enumerate(stage_datas):
            remote_stage = remote_stages[i]
            remote_stage_key = remote_stage_keys[i]
            remote_addr = remote_addrs[i]
            self.stages[remote_stage_key] = InternalStageData(
                sd.stage_id,
                sd.plan_bytes,
                sd.partition_group,
                sd.child_stage_ids,
                sd.num_output_partitions,
                sd.full_partitions,
                remote_stage,
                remote_addr,
            )

        # sort out the mess of who talks to whom and ensure we can supply the correct
        # addresses to each of them
        addrs_by_stage_key = await self.sort_out_addresses()
        if log.level <= logging.DEBUG:
            # TODO: string builder here
            out = ""
            for stage_key, stage in self.stages.items():
                out += f"[{stage_key}]: {stage}\n"
                out += f"child addrs: {addrs_by_stage_key[stage_key]}\n"
            log.debug(out)

        refs = []
        # now tell the stages what they are doing for this query
        for stage_key, isd in self.stages.items():
            log.info(f"going to update plan for {stage_key}")
            kid = addrs_by_stage_key[stage_key]
            refs.append(
                isd.remote_stage.update_plan.remote(
                    isd.stage_id,
                    {stage_id: val["child_addrs"] for (stage_id, val) in kid.items()},
                    isd.partition_group,
                    isd.plan_bytes,
                )
            )
        log.info("that's all of them")

        await wait_for(refs, "updating plans")

    async def sort_out_addresses(self):
        """Iterate through our stages and gather all of their listening addresses.
        Then, provide the addresses to of peer stages to each stage.
        """
        addrs_by_stage_key = {}
        for stage_key, isd in self.stages.items():
            stage_addrs = defaultdict(dict)

            # using "isd" as shorthand to denote InternalStageData as a reminder

            for child_stage_id in isd.child_stage_ids:
                addrs = defaultdict(list)
                child_stage_keys, child_stage_datas = zip(
                    *filter(
                        lambda x: x[1].stage_id == child_stage_id,
                        self.stages.items(),
                    )
                )
                output_partitions = [
                    c_isd.num_output_partitions for c_isd in child_stage_datas
                ]

                # sanity check
                assert all([op == output_partitions[0] for op in output_partitions])
                output_partitions = output_partitions[0]

                for child_stage_isd in child_stage_datas:
                    if child_stage_isd.full_partitions:
                        for partition in range(output_partitions):
                            # this stage is the definitive place to read this output partition
                            addrs[partition] = [child_stage_isd.remote_addr]
                    else:
                        for partition in range(output_partitions):
                            # this output partition must be gathered from all stages with this stage_id
                            addrs[partition] = [
                                c.remote_addr for c in child_stage_datas
                            ]

                stage_addrs[child_stage_id]["child_addrs"] = addrs
                # not necessary but useful for debug logs
                stage_addrs[child_stage_id]["stage_keys"] = child_stage_keys

            addrs_by_stage_key[stage_key] = stage_addrs

        return addrs_by_stage_key

    async def all_done(self):
        await self.pool.all_done()


class RayDataFrame:
    def __init__(
        self,
        ray_internal_df: RayDataFrameInternal,
        supervisor,  # ray.actor.ActorHandle[RayContextSupervisor],
        batch_size=8192,
        partitions_per_worker: int | None = None,
        prefetch_buffer_size=0,
    ):
        self.df = ray_internal_df
        self.supervisor = supervisor
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

            last_stage_id = max([stage.stage_id for stage in self._stages])
            log.debug(f"last stage is {last_stage_id}")

            self.create_ray_stages()

            last_stage_addrs = ray.get(
                self.supervisor.get_stage_addrs.remote(last_stage_id)
            )
            log.debug(f"last stage addrs {last_stage_addrs}")

            reader = self.df.read_final_stage(last_stage_id, last_stage_addrs[0])
            log.debug("got reader")
            self._batches = list(reader)
        return self._batches

    def show(self) -> None:
        batches = self.collect()
        print(prettify(batches))

    def create_ray_stages(self):
        stage_datas = []

        # note, whereas the PyDataFrameStage object contained in self.stages()
        # holds information for a numbered stage,
        # when we tell the supervisor about our query, it wants a StageData
        # object per actor that will be created.  Hence the loop over partition_groups
        for stage in self.stages():
            for partition_group in stage.partition_groups:
                stage_datas.append(
                    StageData(
                        stage.stage_id,
                        stage.plan_bytes(),
                        partition_group,
                        stage.child_stage_ids,
                        stage.num_output_partitions,
                        stage.full_partitions,
                    )
                )

        ref = self.supervisor.new_query.remote(stage_datas)
        call_sync(wait_for([ref], "creating ray stages"))


class RayContext:
    def __init__(
        self,
        batch_size: int = 8192,
        prefetch_buffer_size: int = 0,
        partitions_per_worker: int | None = None,
        worker_pool_min: int = 1,
        worker_pool_max: int = 100,
    ) -> None:
        self.ctx = RayContextInternal()
        self.batch_size = batch_size
        self.partitions_per_worker = partitions_per_worker
        self.prefetch_buffer_size = prefetch_buffer_size

        self.supervisor = RayContextSupervisor.options(
            name="RayContextSupersisor",
        ).remote(
            worker_pool_min,
            worker_pool_max,
        )

        # start up our super visor and don't check in on it until its
        # time to query, then we will await this ref
        start_ref = self.supervisor.start.remote()

        # ensure we are ready
        s = time.time()
        call_sync(wait_for([start_ref], "RayContextSupervisor start"))
        e = time.time()
        log.info(f"RayContext::__init__ waiting for supervisor to be ready took {e-s}s")

    def register_parquet(self, name: str, path: str):
        self.ctx.register_parquet(name, path)

    def register_listing_table(self, name: str, path: str, file_extention="parquet"):
        self.ctx.register_listing_table(name, path, file_extention)

    def sql(self, query: str) -> RayDataFrame:

        df = self.ctx.sql(query)

        return RayDataFrame(
            df,
            self.supervisor,
            self.batch_size,
            self.partitions_per_worker,
            self.prefetch_buffer_size,
        )

    def set(self, option: str, value: str) -> None:
        self.ctx.set(option, value)

    def __del__(self):
        log.info("RayContext, cleaning up remote resources")
        ref = self.supervisor.all_done.remote()
        call_sync(wait_for([ref], "RayContextSupervisor all done"))

    # log.debug("all stage addrs set? or should be")
