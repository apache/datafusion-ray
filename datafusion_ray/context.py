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

import asyncio
import queue
import threading

import datafusion
import ray

from tabulate import tabulate

from datafusion_ray._datafusion_ray_internal import (
    RayContext as RayContextInternal,
)


class DataFusionRayContext:
    def __init__(self) -> None:
        self.ctx = RayContextInternal(RayShuffler())

    def register_parquet(self, name: str, path: str):
        self.ctx.register_parquet(name, path)

    def sql(self, query: str) -> datafusion.DataFrame:
        return self.ctx.sql(query)

    def set(self, option: str, value: str) -> None:
        self.ctx.set(option, value)


class RayIterable:
    def __init__(self, name, actor, partition):
        self.name = name
        self.actor = actor
        self.iterable = actor.stream.remote(partition)

    def __next__(self):
        try:
            object_ref = next(self.iterable)
        except StopIteration as e:
            print(f"{self.name} stop iteration")
            raise e

        # list_of_ref = ray.get(object_ref)

        # print(f"{self.name} ray iterable got list: {list_of_ref}")
        # ob = ray.get(list_of_ref[0])
        ob = ray.get(object_ref)
        print(f"{self.name} got\n{tabulate(ob.to_pandas(), tablefmt='simple_grid')}")
        return ob

    def __iter__(self):
        return self


class RayShuffler:
    def execute_partition(
        self,
        plan: bytes,
        partition: int,
        output_partitions: int,
        input_partitions: int,
        unique_id: str,
    ) -> RayIterable:
        print(f"ray executing partition {partition} for shuffleexec {unique_id}")
        # TODO: make name unique per query tree
        self.actor = RayShuffleActor.options(
            name=f"RayShuffleActor ({unique_id})",
            get_if_exists=True,
        ).remote(unique_id, plan, output_partitions, input_partitions)

        return RayIterable(
            f"RayIterable [{unique_id}] partition:{partition} ", self.actor, partition
        )


@ray.remote(num_cpus=0)
class RayShuffleActor:
    def __init__(
        self, name: str, plan: bytes, output_partitions: int, input_partitions: int
    ) -> None:
        self.name = name
        self.plan = plan
        self.output_partitions = output_partitions
        self.input_partitions = input_partitions

        self.queues = [asyncio.Queue() for _ in range(output_partitions)]

        self.is_finished = [False for _ in range(input_partitions)]

        print(f"creating Actor [{name}] with {output_partitions}, {input_partitions}")

        self._start_partition_tasks()

    def _start_partition_tasks(self):
        ctx = ray.get_runtime_context()
        my_handle = ctx.current_actor

        self.tasks = [
            exec_stream.remote(
                f"{self.name} part:{p}", self.plan, p, self.output_partitions, my_handle
            )
            for p in range(self.input_partitions)
        ]
        print(f"Actor [{self.name}] started tasks: {self.tasks}")

    def finished(self, partition: int) -> None:
        print(f"Actor [{self.name}] finished partition {partition}")
        self.is_finished[partition] = True

        # if we are finished with all input partitions, then signal consumers
        # of our output partitions
        if all(self.is_finished):
            print(f"Actor [{self.name}] totally finished")
            for q in self.queues:
                q.put_nowait(None)

    def put(self, partition: int, thing) -> None:
        self.queues[partition].put_nowait(thing)
        print(f"Actor [{self.name}] received {len(thing)} rows from part {partition}")

    async def get(self, partition: int):
        thing = await self.queues[partition].get()
        return thing

    async def stream(self, partition: int):
        while True:
            thing = await self.get(partition)
            if thing is None:
                break
            yield thing


@ray.remote
def exec_stream(
    name: str,
    plan: bytes,
    shadow_partition: int,
    num_output_partitions: int,
    ray_shuffle_actor,
):

    from datafusion_ray._datafusion_ray_internal import PartitionExecutor

    print(
        f"PartitionExecutor Task [{name}] executing shadow partition {shadow_partition} with {num_output_partitions} output partitions"
    )

    partition_executor = PartitionExecutor(
        f"PartitionExecutor[{name}]", plan, shadow_partition
    )

    class DoAPartition(threading.Thread):
        def __init__(self, partition, reader):
            super().__init__()
            self.partition = partition
            self.reader = reader
            self.refs = []

        def run(self):
            try:
                for batch in self.reader:
                    print(
                        f"PartitionExecutor Task [{name}] got (output part:{self.partition}) \n{tabulate(batch.to_pandas(), tablefmt='simple_grid')}"
                    )
                    # object_ref = ray.put(batch)
                    # ray_shuffle_actor.put.remote(partition, [object_ref])
                    self.refs.append(
                        ray_shuffle_actor.put.remote(self.partition, batch)
                    )
            except Exception as e:
                print(f"PartitionExecutor Task [{name}] got exception {e}")

    threads = []
    for p in range(num_output_partitions):
        reader = partition_executor.output_partition(p)
        t = DoAPartition(p, reader)
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    # ensure all items are enqueued before we call finish below
    all_refs = [ref for t in threads for ref in t.refs]
    print(f"PartitionExecutor Task [{name}] waiting for {all_refs}")
    ray.wait(all_refs, num_returns=len(all_refs), fetch_local=False)

    ray_shuffle_actor.finished.remote(shadow_partition)
