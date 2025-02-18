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

import argparse
import ray
from datafusion import SessionContext, SessionConfig
from datafusion_ray import RayContext, prettify, runtime_env
from datetime import datetime
import json
import os
import time

try:
    import duckdb
except ImportError:
    print(
        "duckdb not installed, which is used in this file for retrieving the TPCH query"
    )
    sys.exit(1)


def tpch_query(qnum: int) -> str:
    query_path = os.path.join(os.path.dirname(__file__), "..", "testdata", "queries")
    return open(os.path.join(query_path, f"q{qnum}.sql")).read()


def main(
    qnum: int,
    data_path: str,
    concurrency: int,
    batch_size: int,
    partitions_per_worker: int | None,
    worker_pool_min: int,
    listing_tables: bool,
    validate: bool,
    prefetch_buffer_size: int,
):

    # Register the tables
    table_names = [
        "customer",
        "lineitem",
        "nation",
        "orders",
        "part",
        "partsupp",
        "region",
        "supplier",
    ]
    # Connect to a cluster
    # use ray job submit
    ray.init(runtime_env=runtime_env)

    ctx = RayContext(
        batch_size=batch_size,
        partitions_per_worker=partitions_per_worker,
        prefetch_buffer_size=prefetch_buffer_size,
        worker_pool_min=worker_pool_min,
    )

    ctx.set("datafusion.execution.target_partitions", f"{concurrency}")
    # ctx.set("datafusion.execution.parquet.pushdown_filters", "true")
    ctx.set("datafusion.optimizer.enable_round_robin_repartition", "false")
    ctx.set("datafusion.execution.coalesce_batches", "false")

    local_config = SessionConfig()

    local_ctx = SessionContext(local_config)

    for table in table_names:
        path = os.path.join(data_path, f"{table}.parquet")
        print(f"Registering table {table} using path {path}")
        if listing_tables:
            ctx.register_listing_table(table, f"{path}/")
            local_ctx.register_listing_table(table, f"{path}/")
        else:
            ctx.register_parquet(table, path)
            local_ctx.register_parquet(table, path)

    current_time_millis = int(datetime.now().timestamp() * 1000)
    results_path = f"datafusion-ray-tpch-{current_time_millis}.json"
    print(f"Writing results to {results_path}")

    results = {
        "engine": "datafusion-ray",
        "benchmark": "tpch",
        "settings": {
            "concurrency": concurrency,
            "batch_size": batch_size,
            "prefetch_buffer_size": prefetch_buffer_size,
            "partitions_per_worker": partitions_per_worker,
        },
        "data_path": data_path,
        "queries": {},
    }
    if validate:
        results["local_queries"] = {}
        results["validated"] = {}

    duckdb.sql("load tpch")

    queries = range(1, 23) if qnum == -1 else [qnum]
    for qnum in queries:
        sql = tpch_query(qnum)

        statements = sql.split(";")
        sql = statements[0]

        print("executing ", sql)

        start_time = time.time()
        df = ctx.sql(sql)
        end_time = time.time()
        print("Logical plan \n", df.logical_plan().display_indent())
        print("Optimized Logical plan \n", df.optimized_logical_plan().display_indent())
        part1 = end_time - start_time
        for stage in df.stages():
            print(
                f"Stage {stage.stage_id} output partitions:{stage.num_output_partitions} partition_groups: {stage.partition_groups} full_partitions: {stage.full_partitions}"
            )
            print(stage.display_execution_plan())

        start_time = time.time()
        batches = df.collect()
        end_time = time.time()
        results["queries"][qnum] = end_time - start_time + part1

        calculated = prettify(batches)
        print(calculated)
        if validate:
            start_time = time.time()
            answer_batches = local_ctx.sql(sql).collect()
            end_time = time.time()
            results["local_queries"][qnum] = end_time - start_time

            expected = prettify(answer_batches)

            results["validated"][qnum] = calculated == expected
        print(f"done with query {qnum}")

        # write the results as we go, so you can peek at them
        results_dump = json.dumps(results, indent=4)
        with open(results_path, "w+") as f:
            f.write(results_dump)

        # write results to stdout
        print(results_dump)

    # give ray a moment to clean up
    print("sleeping for 3 seconds for ray to clean up")
    time.sleep(3)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="DataFusion benchmark derived from TPC-H / TPC-DS"
    )
    parser.add_argument("--data", required=True, help="Path to data files")
    parser.add_argument(
        "--concurrency", required=True, help="Number of concurrent tasks"
    )
    parser.add_argument("--qnum", type=int, default=-1, help="TPCH query number, 1-22")
    parser.add_argument("--listing-tables", action="store_true")
    parser.add_argument("--validate", action="store_true")
    parser.add_argument(
        "--log-level", default="INFO", help="ERROR,WARN,INFO,DEBUG,TRACE"
    )
    parser.add_argument(
        "--batch-size",
        required=False,
        default=8192,
        help="Desired batch size output per stage",
    )
    parser.add_argument(
        "--partitions-per-worker",
        type=int,
        help="Max partitions per Stage Service Worker",
    )
    parser.add_argument(
        "--prefetch-buffer-size",
        required=False,
        default=0,
        type=int,
        help="How many batches each stage should eagerly buffer",
    )
    parser.add_argument(
        "--worker-pool-min",
        type=int,
        help="Minimum number of RayStages to keep in pool",
    )

    args = parser.parse_args()

    main(
        args.qnum,
        args.data,
        int(args.concurrency),
        int(args.batch_size),
        args.partitions_per_worker,
        args.worker_pool_min,
        args.listing_tables,
        args.validate,
        args.prefetch_buffer_size,
    )
