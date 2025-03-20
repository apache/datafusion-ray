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
from datafusion_ray.util import LocalValidator, prettify
from datetime import datetime
import json
import os
import time


def tpch_query(qnum: int) -> str:
    query_path = os.path.join(os.path.dirname(__file__), "queries")
    return open(os.path.join(query_path, f"q{qnum}.sql")).read()


def main(
    qnum: int,
    data_path: str,
    concurrency: int,
    batch_size: int,
    partitions_per_processor: int | None,
    processor_pool_min: int,
    listing_tables: bool,
    validate: bool,
    output_path: str,
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
        partitions_per_processor=partitions_per_processor,
        prefetch_buffer_size=prefetch_buffer_size,
        processor_pool_min=processor_pool_min,
        processor_pool_max=1000,
    )

    local = LocalValidator()

    ctx.set("datafusion.execution.target_partitions", f"{concurrency}")
    # ctx.set("datafusion.execution.parquet.pushdown_filters", "true")
    ctx.set("datafusion.optimizer.enable_round_robin_repartition", "false")
    ctx.set("datafusion.execution.coalesce_batches", "false")

    for table in table_names:
        path = os.path.join(data_path, f"{table}.parquet")
        print(f"Registering table {table} using path {path}")
        if listing_tables:
            ctx.register_listing_table(table, path)
            local.register_listing_table(table, path)
        else:
            ctx.register_parquet(table, path)
            local.register_parquet(table, path)

    current_time_millis = int(datetime.now().timestamp() * 1000)
    results_path = os.path.join(
        output_path, f"datafusion-ray-tpch-{current_time_millis}.json"
    )
    print(f"Writing results to {results_path}")

    results = {
        "engine": "datafusion-ray",
        "benchmark": "tpch",
        "settings": {
            "concurrency": concurrency,
            "batch_size": batch_size,
            "prefetch_buffer_size": prefetch_buffer_size,
            "partitions_per_processor": partitions_per_processor,
        },
        "data_path": data_path,
        "queries": {},
    }
    if validate:
        results["validated"] = {}

    queries = range(1, 23) if qnum == -1 else [qnum]
    for qnum in queries:
        sql = tpch_query(qnum)

        statements = list(
            filter(lambda x: len(x) > 0, map(lambda x: x.strip(), sql.split(";")))
        )

        start_time = time.time()
        all_batches = []
        for sql in statements:
            print("executing ", sql)
            df = ctx.sql(sql)
            all_batches.append(df.collect())
        end_time = time.time()
        results["queries"][qnum] = end_time - start_time

        calculated = "\n".join([prettify(b) for b in all_batches])
        print(calculated)
        out_path = os.path.join(
            output_path, f"datafusion_ray_tpch_q{qnum}_result.txt"
        )
        with open(out_path, "w") as f:
            f.write(calculated)

        if validate:
            all_batches = []
            for sql in statements:
                all_batches.append(local.collect_sql(sql))
            expected = "\n".join([prettify(b) for b in all_batches])

            results["validated"][qnum] = calculated == expected
        print(f"done with query {qnum}")

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
        "--partitions-per-processor",
        type=int,
        help="partitions per DFRayProcessor",
    )
    parser.add_argument(
        "--output-path",
        type=str,
        default=".",
        help="directory to write output json",
    )

    parser.add_argument(
        "--prefetch-buffer-size",
        required=False,
        default=0,
        type=int,
        help="How many batches each stage should eagerly buffer",
    )
    parser.add_argument(
        "--processor-pool-min",
        type=int,
        help="Minimum number of DFRayProcessors to keep in pool",
    )

    args = parser.parse_args()

    main(
        args.qnum,
        args.data,
        int(args.concurrency),
        int(args.batch_size),
        args.partitions_per_processor,
        args.processor_pool_min,
        args.listing_tables,
        args.validate,
        args.output_path,
        args.prefetch_buffer_size,
    )
