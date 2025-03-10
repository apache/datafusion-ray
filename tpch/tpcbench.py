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
from datafusion_ray import DFRayContext, df_ray_runtime_env
from datafusion_ray.util import exec_sqls_on_tables, prettify
from datetime import datetime
import json
import os
import time


def tpch_query(qnum: int) -> str:
    query_path = os.path.join(os.path.dirname(__file__), "queries")
    return open(os.path.join(query_path, f"q{qnum}.sql")).read()


def main(
    queries: list[(str, str)],
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
    ray.init(runtime_env=df_ray_runtime_env)

    ctx = DFRayContext(
        batch_size=batch_size,
        partitions_per_worker=partitions_per_worker,
        prefetch_buffer_size=prefetch_buffer_size,
        worker_pool_min=worker_pool_min,
    )

    ctx.set("datafusion.execution.target_partitions", f"{concurrency}")
    # ctx.set("datafusion.execution.parquet.pushdown_filters", "true")
    ctx.set("datafusion.optimizer.enable_round_robin_repartition", "false")
    ctx.set("datafusion.execution.coalesce_batches", "false")

    for table in table_names:
        path = os.path.join(data_path, f"{table}.parquet")
        print(f"Registering table {table} using path {path}")
        if listing_tables:
            ctx.register_listing_table(table, path)
        else:
            ctx.register_parquet(table, path)

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
        results["validated"] = {}

    for (qid, sql) in queries:
        print("executing ", sql)

        statements = [s for s in sql.split(";") if s.strip() != ""]
        start_time = time.time()
        batches = [ctx.sql(s).collect() for s in statements]
        end_time = time.time()
        results["queries"][qid] = end_time - start_time

        calculated = [prettify(batch) for batch in batches if batch]
        for pretty_batch in calculated:
            print(pretty_batch)
        if validate:
            tables = [
                (name, os.path.join(data_path, f"{name}.parquet"))
                for name in table_names
            ]
            answer_batches = [b for b in exec_sqls_on_tables(
                statements, tables, listing_tables) if b]

            validated = True
            if len(answer_batches) == len(calculated):
                expected = [prettify([answer_batch])
                            for answer_batch in answer_batches]
                validated = all(x[0] == x[1]
                                for x in zip(calculated, expected))
                for x in zip(calculated, expected):
                    if x[0] != x[1]:
                        print(f"Expected:\n{x[1]}")
                        print(f"Got:\n{x[0]}")
            else:
                print(
                    f"Expected {len(answer_batches)} batches, got {len(calculated)}")
                validated = False

            results["validated"][qid] = validated
        print(f"done with query {qid}")

        # write the results as we go, so you can peek at them
        results_dump = json.dumps(results, indent=4)
        with open(results_path, "w+") as f:
            f.write(results_dump)

        # write results to stdout
        print(results_dump)

    # give ray a moment to clean up
    print("benchmark complete. sleeping for 3 seconds for ray to clean up")
    time.sleep(3)

    if validate and False in results["validated"].values():
        # return a non zero return code if we did not validate all queries
        print("Possible incorrect query result")
        exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="DataFusion benchmark derived from TPC-H / TPC-DS"
    )
    parser.add_argument("--data", required=True, help="Path to data files")
    parser.add_argument(
        "--concurrency", required=True, help="Number of concurrent tasks"
    )
    parser.add_argument("--qnum", type=int, default=-1,
                        help="TPCH query number, 1-22")
    parser.add_argument("--query", required=False, type=str,
                        help="Custom query to run with tpch tables")
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
        "--partitions-per-processor",
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

    if (args.qnum != -1 and args.query is not None):
        print("Please specify either --qnum or --query, but not both")

    queries = []
    if (args.qnum != -1):
        if args.qnum < 1 or args.qnum > 22:
            print("Invalid query number. Please specify a number between 1 and 22.")
            exit(1)
        else:
            queries.append((str(args.qnum), tpch_query(args.qnum)))
            print("Executing tpch query ", args.qnum)

    elif (args.query is not None):
        queries.append(("custom query", args.query))
        print("Executing custom query ", args.query)
    else:
        print("Executing all tpch queries")
        queries = [(str(i), tpch_query(i)) for i in range(1, 23)]

    main(
        queries,
        args.data,
        int(args.concurrency),
        int(args.batch_size),
        args.partitions_per_processor,
        args.worker_pool_min,
        args.listing_tables,
        args.validate,
        args.prefetch_buffer_size,
    )
