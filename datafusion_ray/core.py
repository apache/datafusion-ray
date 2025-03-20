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
import time

from .friendly import new_friendly_name

from datafusion_ray._datafusion_ray_internal import (
    DFRayContext as DFRayContextInternal,
    DFRayDataFrame as DFRayDataFrameInternal,
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
df_ray_runtime_env = {
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
    done, _ = await asyncio.wait(
        [asyncio.create_task(_ensure_coro(c)) for c in coros]
    )
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


class DFRayProcessorPool:
    """A pool of DFRayProcessor actors that can be acquired and released"""

    # TODO: We can probably manage this set in a better way
    # This is not a threadsafe implementation, though the DFRayContextSupervisor accesses it
    # from a single thread
    #
    # This is simple though and will suffice for now

    def __init__(self, min_processors: int, max_processors: int):
        self.min_processors = min_processors
        self.max_processors = max_processors

        # a map of processor_key (a random identifier) to stage actor reference
        self.pool = {}
        # a map of processor_key to listening address
        self.addrs = {}

        # holds object references from the start_up method for each processor
        # we know all processors are listening when all of these refs have
        # been waited on.  When they are ready we remove them from this set
        self.processors_started = set()

        # an event that is set when all processors are ready to serve
        self.processors_ready = asyncio.Event()

        # processors that are started but we need to get their address
        self.need_address = set()

        # processors that we have the address for but need to start serving
        self.need_serving = set()

        # processors in use
        self.acquired = set()

        # processors available
        self.available = set()

        for _ in range(min_processors):
            self._new_processor()

        log.info(
            f"created ray processor pool (min_processors: {min_processors}, max_processors: {max_processors})"
        )

    async def start(self):
        if not self.processors_ready.is_set():
            await self._wait_for_processors_started()
            await self._wait_for_get_addrs()
            await self._wait_for_serve()
            self.processors_ready.set()

    async def wait_for_ready(self):
        await self.processors_ready.wait()

    async def acquire(self, need=1):
        processor_keys = []

        have = len(self.available)
        total = len(self.available) + len(self.acquired)
        can_make = self.max_processors - total

        need_to_make = need - have

        if need_to_make > can_make:
            raise Exception(
                f"Cannot allocate processors above {self.max_processors}"
            )

        if need_to_make > 0:
            log.debug(f"creating {need_to_make} additional processors")
            for _ in range(need_to_make):
                self._new_processor()
            await wait_for([self.start()], "waiting for created processors")

        assert len(self.available) >= need

        for _ in range(need):
            processor_key = self.available.pop()
            self.acquired.add(processor_key)

            processor_keys.append(processor_key)

        processors = [self.pool[sk] for sk in processor_keys]
        addrs = [self.addrs[sk] for sk in processor_keys]
        return (processors, processor_keys, addrs)

    def release(self, processor_keys: list[str]):
        for processor_key in processor_keys:
            self.acquired.remove(processor_key)
            self.available.add(processor_key)

    def _new_processor(self):
        self.processors_ready.clear()
        processor_key = new_friendly_name()
        log.debug(f"starting processor: {processor_key}")
        processor = DFRayProcessor.options(
            name=f"Processor : {processor_key}"
        ).remote(processor_key)
        self.pool[processor_key] = processor
        self.processors_started.add(processor.start_up.remote())
        self.available.add(processor_key)

    async def _wait_for_processors_started(self):
        log.info("waiting for processors to be ready")
        started_keys = await wait_for(
            self.processors_started, "processors to be started"
        )
        # we need the addresses of these processors still
        self.need_address.update(set(started_keys))
        # we've started all the processors we know about
        self.processors_started = set()
        log.info("processors are all listening")

    async def _wait_for_get_addrs(self):
        # get the addresses in a pipelined fashion
        refs = []
        processor_keys = []
        for processor_key in self.need_address:
            processor = self.pool[processor_key]
            refs.append(processor.addr.remote())
            processor_keys.append(processor_key)

            self.need_serving.add(processor_key)

        addrs = await wait_for(refs, "processor addresses")

        for key, addr in addrs:
            self.addrs[key] = addr

        self.need_address = set()

    async def _wait_for_serve(self):
        log.info("running processors")
        try:
            for processor_key in self.need_serving:
                log.info(f"starting serving of processor {processor_key}")
                processor = self.pool[processor_key]
                processor.serve.remote()
            self.need_serving = set()

        except Exception as e:
            log.error(f"ProcessorPool: Uhandled Exception in serve: {e}")
            raise e

    async def all_done(self):
        log.info("calling processor all done")
        refs = [
            processor.all_done.remote() for processor in self.pool.values()
        ]
        await wait_for(refs, "processors to be all done")
        log.info("all processors shutdown")


@ray.remote(num_cpus=0.01, scheduling_strategy="SPREAD")
class DFRayProcessor:
    def __init__(self, processor_key):
        self.processor_key = processor_key

        # import this here so ray doesn't try to serialize the rust extension
        from datafusion_ray._datafusion_ray_internal import (
            DFRayProcessorService,
        )

        self.processor_service = DFRayProcessorService(processor_key)

    async def start_up(self):
        # this method is sync
        self.processor_service.start_up()
        return self.processor_key

    async def all_done(self):
        await self.processor_service.all_done()

    async def addr(self):
        return (self.processor_key, self.processor_service.addr())

    async def update_plan(
        self,
        stage_id: int,
        stage_addrs: dict[int, dict[int, list[str]]],
        partition_group: list[int],
        plan_bytes: bytes,
    ):
        await self.processor_service.update_plan(
            stage_id,
            stage_addrs,
            partition_group,
            plan_bytes,
        )

    async def serve(self):
        log.info(
            f"[{self.processor_key}] serving on {self.processor_service.addr()}"
        )
        await self.processor_service.serve()
        log.info(f"[{self.processor_key}] done serving")


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
    remote_processor: ...  # ray.actor.ActorHandle[DFRayProcessor]
    remote_addr: str

    def __str__(self):
        return f"""Stage: {self.stage_id}, pg: {self.partition_group}, child_stages:{self.child_stage_ids}, listening addr:{self.remote_addr}"""


@ray.remote(num_cpus=0.01, scheduling_strategy="SPREAD")
class DFRayContextSupervisor:
    def __init__(
        self,
        processor_pool_min: int,
        processor_pool_max: int,
    ) -> None:
        log.info(
            f"Creating DFRayContextSupervisor processor_pool_min: {processor_pool_min}"
        )
        self.pool = DFRayProcessorPool(processor_pool_min, processor_pool_max)
        self.stages: dict[str, InternalStageData] = {}
        log.info("Created DFRayContextSupervisor")

    async def start(self):
        await self.pool.start()

    async def wait_for_ready(self):
        await self.pool.wait_for_ready()

    async def get_stage_addrs(self, stage_id: int):
        addrs = [
            sd.remote_addr
            for sd in self.stages.values()
            if sd.stage_id == stage_id
        ]
        return addrs

    async def new_query(
        self,
        stage_datas: list[StageData],
    ):
        if len(self.stages) > 0:
            self.pool.release(list(self.stages.keys()))

        remote_processors, remote_processor_keys, remote_addrs = (
            await self.pool.acquire(len(stage_datas))
        )
        self.stages = {}

        for i, sd in enumerate(stage_datas):
            remote_processor = remote_processors[i]
            remote_processor_key = remote_processor_keys[i]
            remote_addr = remote_addrs[i]
            self.stages[remote_processor_key] = InternalStageData(
                sd.stage_id,
                sd.plan_bytes,
                sd.partition_group,
                sd.child_stage_ids,
                sd.num_output_partitions,
                sd.full_partitions,
                remote_processor,
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
                isd.remote_processor.update_plan.remote(
                    isd.stage_id,
                    {
                        stage_id: val["child_addrs"]
                        for (stage_id, val) in kid.items()
                    },
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
                assert all(
                    [op == output_partitions[0] for op in output_partitions]
                )
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


class DFRayDataFrame:
    def __init__(
        self,
        internal_df: DFRayDataFrameInternal,
        supervisor,  # ray.actor.ActorHandle[DFRayContextSupervisor],
        batch_size=8192,
        partitions_per_processor: int | None = None,
        prefetch_buffer_size=0,
    ):
        self.df = internal_df
        self.supervisor = supervisor
        self._stages = None
        self._batches = None
        self.batch_size = batch_size
        self.partitions_per_processor = partitions_per_processor
        self.prefetch_buffer_size = prefetch_buffer_size

    def stages(self):
        # create our coordinator now, which we need to create stages
        if not self._stages:
            self._stages = self.df.stages(
                self.batch_size,
                self.prefetch_buffer_size,
                self.partitions_per_processor,
            )

        return self._stages

    def schema(self):
        return self.df.schema()

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
            log.debug(f"creating stages took {t2 - t1}s")

            last_stage_id = max([stage.stage_id for stage in self._stages])
            log.debug(f"last stage is {last_stage_id}")

            self.create_ray_stages()

            last_stage_addrs = ray.get(
                self.supervisor.get_stage_addrs.remote(last_stage_id)
            )
            log.debug(f"last stage addrs {last_stage_addrs}")

            reader = self.df.read_final_stage(
                last_stage_id, last_stage_addrs[0]
            )
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


class DFRayContext:
    def __init__(
        self,
        batch_size: int = 8192,
        prefetch_buffer_size: int = 0,
        partitions_per_processor: int | None = None,
        processor_pool_min: int = 1,
        processor_pool_max: int = 100,
    ) -> None:
        self.ctx = DFRayContextInternal()
        self.batch_size = batch_size
        self.partitions_per_processor = partitions_per_processor
        self.prefetch_buffer_size = prefetch_buffer_size

        self.supervisor = DFRayContextSupervisor.options(
            name="RayContextSupersisor",
        ).remote(
            processor_pool_min,
            processor_pool_max,
        )

        # start up our super visor and don't check in on it until its
        # time to query, then we will await this ref
        start_ref = self.supervisor.start.remote()

        # ensure we are ready
        s = time.time()
        call_sync(wait_for([start_ref], "RayContextSupervisor start"))
        e = time.time()
        log.info(
            f"RayContext::__init__ waiting for supervisor to be ready took {e - s}s"
        )

    def register_parquet(self, name: str, path: str):
        """
        Register a Parquet file with the given name and path.
        The path can be a local filesystem path, absolute filesystem path, or a url.

        If the path is a object store url, the appropriate object store will be registered.
        Configuration of the object store will be gathered from the environment.

        For example for s3:// urls, credentials will be looked for by the AWS SDK,
        which will check environment variables, credential files, etc

        Parameters:
        path (str): The file path to the Parquet file.
        name (str): The name to register the Parquet file under.
        """
        self.ctx.register_parquet(name, path)

    def register_csv(self, name: str, path: str):
        """
        Register a csvfile with the given name and path.
        The path can be a local filesystem path, absolute filesystem path, or a url.

        If the path is a object store url, the appropriate object store will be registered.
        Configuration of the object store will be gathered from the environment.

        For example for s3:// urls, credentials will be looked for by the AWS SDK,
        which will check environment variables, credential files, etc

        Parameters:
        path (str): The file path to the csv file.
        name (str): The name to register the Parquet file under.
        """
        self.ctx.register_csv(name, path)

    def register_listing_table(
        self, name: str, path: str, file_extention="parquet"
    ):
        """
        Register a directory of parquet files with the given name.
        The path can be a local filesystem path, absolute filesystem path, or a url.

        If the path is a object store url, the appropriate object store will be registered.
        Configuration of the object store will be gathered from the environment.

        For example for s3:// urls, credentials will be looked for by the AWS SDK,
        which will check environment variables, credential files, etc

        Parameters:
        path (str): The file path to the Parquet file directory
        name (str): The name to register the Parquet file under.
        """

        self.ctx.register_listing_table(name, path, file_extention)

    def sql(self, query: str) -> DFRayDataFrame:

        df = self.ctx.sql(query)

        return DFRayDataFrame(
            df,
            self.supervisor,
            self.batch_size,
            self.partitions_per_processor,
            self.prefetch_buffer_size,
        )

    def set(self, option: str, value: str) -> None:
        self.ctx.set(option, value)

    def __del__(self):
        log.info("DFRayContext, cleaning up remote resources")
        ref = self.supervisor.all_done.remote()
        call_sync(wait_for([ref], "DFRayContextSupervisor all done"))
