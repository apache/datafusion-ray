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

import json
import os
import time
from typing import Iterable

import pyarrow as pa
import ray

import datafusion_ray
from datafusion_ray import Context, ExecutionGraph, QueryStage
from typing import List, Any
from datafusion import SessionContext


@ray.remote(num_cpus=0)
def execute_query_stage(
    query_stages: list[QueryStage],
    stage_id: int
) -> tuple[int, list[ray.ObjectRef]]:
    """
    Execute a query stage on the workers.

    Returns the stage ID, and a list of futures for the output partitions of the query stage.
    """
    stage = QueryStage(stage_id, query_stages[stage_id])

    # execute child stages first
    child_futures = []
    for child_id in stage.get_child_stage_ids():
        child_futures.append(
            execute_query_stage.remote(query_stages, child_id)
        )

    # if the query stage has a single output partition then we need to execute for the output
    # partition, otherwise we need to execute in parallel for each input partition
    concurrency = stage.get_input_partition_count()
    output_partitions_count = stage.get_output_partition_count()
    if output_partitions_count == 1:
        # reduce stage
        print("Forcing reduce stage concurrency from {} to 1".format(concurrency))
        concurrency = 1

    print(
        "Scheduling query stage #{} with {} input partitions and {} output partitions".format(
            stage.id(), concurrency, output_partitions_count
        )
    )

    # A list of (stage ID, list of futures) for each child stage
    # Each list is a 2-D array of (input partitions, output partitions).
    child_outputs = ray.get(child_futures)

    # if we are using disk-based shuffle, wait until the child stages to finish
    # writing the shuffle files to disk first.
    ray.get([f for _, lst in child_outputs for f in lst])

    # schedule the actual execution workers
    plan_bytes = stage.get_execution_plan_bytes()
    futures = []
    opt = {}
    for part in range(concurrency):
        futures.append(
            execute_query_partition.options(**opt).remote(
                stage_id, plan_bytes, part
            )
        )

    return stage_id, futures


@ray.remote
def execute_query_partition(
    stage_id: int,
    plan_bytes: bytes,
    part: int
) -> Iterable[pa.RecordBatch]:
    start_time = time.time()
    # plan = datafusion_ray.deserialize_execution_plan(plan_bytes)
    # print(
    #     "Worker executing plan {} partition #{} with shuffle inputs {}".format(
    #         plan.display(),
    #         part,
    #         input_partition_ids,
    #     )
    # )
    # This is delegating to DataFusion for execution, but this would be a good place
    # to plug in other execution engines by translating the plan into another engine's plan
    # (perhaps via Substrait, once DataFusion supports converting a physical plan to Substrait)
    ret = datafusion_ray.execute_partition(plan_bytes, part)
    duration = time.time() - start_time
    event = {
        "cat": f"{stage_id}-{part}",
        "name": f"{stage_id}-{part}",
        "pid": ray.util.get_node_ip_address(),
        "tid": os.getpid(),
        "ts": int(start_time * 1_000_000),
        "dur": int(duration * 1_000_000),
        "ph": "X",
    }
    print(json.dumps(event), end=",")
    return ret[0] if len(ret) == 1 else ret


class DatafusionRayContext:
    def __init__(self, df_ctx: SessionContext):
        self.df_ctx = df_ctx
        self.ctx = Context(df_ctx)

    def register_csv(self, table_name: str, path: str, has_header: bool):
        self.ctx.register_csv(table_name, path, has_header)

    def register_parquet(self, table_name: str, path: str):
        self.ctx.register_parquet(table_name, path)

    def register_data_lake(self, table_name: str, paths: List[str]):
        self.ctx.register_datalake_table(table_name, paths)

    def sql(self, sql: str) -> pa.RecordBatch:
        # TODO we should parse sql and inspect the plan rather than
        # perform a string comparison here
        sql_str = sql.lower()
        if "create view" in sql_str or "drop view" in sql_str:
            self.ctx.sql(sql)
            return []

        df = self.df_ctx.sql(sql)
        execution_plan = df.execution_plan()

        graph = self.ctx.plan(execution_plan)
        final_stage_id = graph.get_final_query_stage().id()

        # serialize the query stages and store in Ray object store
        query_stages = [
                graph.get_query_stage(i).get_execution_plan_bytes()
            for i in range(final_stage_id + 1)
        ]
        # schedule execution
        future = execute_query_stage.remote(
            query_stages,
            final_stage_id
        )
        _, partitions = ray.get(future)
        # assert len(partitions) == 1, len(partitions)
        result_set = ray.get(partitions[0])
        return result_set
