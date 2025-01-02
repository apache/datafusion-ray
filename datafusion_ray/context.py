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
import threading

import datafusion
import pyarrow as pa
import ray

from datafusion_ray._datafusion_ray_internal import (
    RayContext as RayContextInternal,
    internal_execute_partition,
)


class RayContext:
    def __init__(self) -> None:
        self.ctx = RayContextInternal(RayShuffler())

    def register_parquet(self, name: str, path: str):
        self.ctx.register_parquet(name, path)

    def sql(self, query: str) -> datafusion.DataFrame:
        return self.ctx.sql(query)

    def set(self, option: str, value: str) -> None:
        self.ctx.set(option, value)


class RayIterable:
    def __init__(self, iterable, name):
        self.iterable = iterable
        self.name = name

    def __next__(self):
        print(f"{self.name} ray iterable getting next")
        object_ref = next(self.iterable)

        list_of_ref = ray.get(object_ref)

        if list_of_ref is None:
            print(f"{self.name} ray iterable got None")
            raise StopIteration

        print(f"{self.name} ray iterable got list: {list_of_ref}")
        ob = ray.get(list_of_ref[0])
        print(f"{self.name} ray iterable got object ")
        return ob

    def __iter__(self):
        return self


class RayShuffler:
    def __init__(self):
        pass

    def execute_partition(
        self,
        plan: bytes,
        partition: int,
        output_partitions: int,
        input_partitions: int,
        unique_id: str,
    ) -> RayIterable:
        print(f"ray executing partition {partition}")
        # TODO: make name unique per query tree
        self.actor = RayShuffleActor.options(
            name=f"RayShuffleActor ({unique_id})",
            # lifetime="detached",
            get_if_exists=True,
        ).remote(plan, output_partitions, input_partitions)

        stream = exec_stream(self.actor, partition)
        return RayIterable(stream, f"partition {partition} ")


def exec_stream(actor, partition: int):
    while True:
        object_ref = actor.stream.remote(partition)
        print(f"stream got {object_ref}")
        if object_ref is None:
            print("breaking")
            break
        print(f"yielding {object_ref}")
        yield object_ref


@ray.remote
class RayShuffleActor:
    def __init__(
        self, plan: bytes, output_partitions: int, input_partitions: int
    ) -> None:
        self.plan = plan
        self.output_partitions = output_partitions
        self.input_partitions = input_partitions

        self.queues = [asyncio.Queue() for _ in range(output_partitions)]

        self.is_finished = [False for _ in range(input_partitions)]

        print(f"creating actor with {output_partitions}, {input_partitions}")

        self._start_partition_tasks()

    def _start_partition_tasks(self):
        ctx = ray.get_runtime_context()
        my_handle = ctx.current_actor

        self.tasks = [
            _exec_stream.remote(self.plan, p, self.output_partitions, my_handle)
            for p in range(self.input_partitions)
        ]
        print(f"started tasks: {self.tasks}")

    def finished(self, partition: int) -> None:
        self.is_finished[partition] = True

        # if we are finished with all input partitions, then signal consumers
        # of our output partitions
        if all(self.is_finished):
            for q in self.queues:
                q.put_nowait(None)
        print(f"Actor finished partition {partition}")

    async def put(self, partition: int, thing) -> None:
        await self.queues[partition].put(thing)

    async def stream(self, partition: int):
        thing = await self.queues[partition].get()
        return thing


@ray.remote
def _exec_stream(
    plan: bytes, shadow_partition: int, output_partitions: int, ray_shuffle_actor
):
    my_id = ray.get_runtime_context().get_task_id()
    print(f"Task {my_id} executing shadow partition {shadow_partition}")
    print(f"Task {my_id} ray_shuffle handle: {ray_shuffle_actor}")

    def do_a_partition(partition):
        reader: pa.RecordBatchReader = internal_execute_partition(
            plan, partition, shadow_partition
        )

        print(f"Task {my_id} got reader for partition {partition}")

        for batch in reader:
            record_batch: pa.RecordBatch = batch
            print(f"Task {my_id} got batch {len(record_batch)} rows")

            object_ref = ray.put(batch)
            ray_shuffle_actor.put.remote(partition, [object_ref])

    threads = []
    for p in range(output_partitions):
        t = threading.Thread(target=do_a_partition, args=(p,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    ray_shuffle_actor.finished.remote(shadow_partition)
