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
from datafusion_ray import RayContext, prettify
from datetime import datetime
import json
import os
import time

import duckdb
from datafusion.object_store import AmazonS3


def main(
    qnum: int,
    data_path: str,
    concurrency: int,
    batch_size: int,
    isolate_partitions: bool,
    listing_tables: bool,
    validate: bool,
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
    ray.init()

    ctx = RayContext(
        batch_size=batch_size,
        isolate_partitions=isolate_partitions,
        bucket="rob-tandy-tmp",
    )

    ctx.set("datafusion.execution.target_partitions", f"{concurrency}")
    # ctx.set("datafusion.execution.parquet.pushdown_filters", "true")
    # ctx.set("datafusion.optimizer.enable_round_robin_repartition", "false")
    ctx.set("datafusion.execution.coalesce_batches", "false")

    local_config = SessionConfig()
    local_config.set("datafusion.execution.target_partitions", f"{concurrency}")
    # local_config.set("datafusion.execution.parquet.pushdown_filters", "true")
    # local_config.set("datafusion.optimizer.enable_round_robin_repartition", "false")
    local_config.set("datafusion.execution.coalesce_batches", "false")

    local_ctx = SessionContext(local_config)
    local_ctx.register_object_store("s3://", AmazonS3(bucket_name="rob-tandy-tmp"))

    for table in table_names:
        path = os.path.join(data_path, f"{table}.parquet")
        print(f"Registering table {table} using path {path}")
        if listing_tables:
            ctx.register_listing_table(table, f"{path}/")
            local_ctx.register_listing_table(table, f"{path}/")
        else:
            ctx.register_parquet(table, path)
            local_ctx.register_parquet(table, path)

    results = {
        "engine": "datafusion-python",
        "benchmark": "tpch",
        "data_path": data_path,
        "queries": {},
    }

    duckdb.sql("load tpch")

    queries = range(1, 23) if qnum == -1 else [qnum]
    for qnum in queries:
        print("Running query ", qnum)
        sql: str = duckdb.sql(
            f"select * from tpch_queries() where query_nr=?", params=(qnum,)
        ).df()["query"][0]

        print("executing ", sql)

        df = ctx.sql(sql)
        start_time = time.time()
        end_time = time.time()
        part1 = end_time - start_time
        for stage in df.stages():
            print("Stage ", stage.stage_id)
            print(stage.execution_plan().display_indent())

        start_time = time.time()
        batches = df.collect()
        end_time = time.time()

        calculated = prettify(batches)
        print(calculated)
        if validate:
            answer_batches = local_ctx.sql(sql).collect()
            expected = prettify(answer_batches)

            if not calculated == expected:
                print(f"Possible wrong answer for TPCH query {qnum}")
                print(expected)
                raise Exception("Wrong answer")
        results["queries"][qnum] = [end_time - start_time + part1]

    results = json.dumps(results, indent=4)
    current_time_millis = int(datetime.now().timestamp() * 1000)
    results_path = f"datafusion-ray-tpch-{current_time_millis}.json"
    print(f"Writing results to {results_path}")
    # with open(results_path, "w") as f:
    #    f.write(results)

    # write results to stdout
    print(results)

    time.sleep(3)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="DataFusion benchmark derived from TPC-H / TPC-DS"
    )
    parser.add_argument("--data", required=True, help="Path to data files")
    parser.add_argument(
        "--concurrency", required=True, help="Number of concurrent tasks"
    )
    parser.add_argument("--isolate", action="store_true")
    parser.add_argument("--qnum", type=int, default=-1, help="TPCH query number, 1-22")
    parser.add_argument("--listing-tables", action="store_true")
    parser.add_argument("--validate", action="store_true")
    parser.add_argument(
        "--batch-size",
        required=False,
        default=8192,
        help="Desired batch size output per stage",
    )
    args = parser.parse_args()

    main(
        args.qnum,
        args.data,
        int(args.concurrency),
        int(args.batch_size),
        args.isolate,
        args.listing_tables,
        args.validate,
    )
