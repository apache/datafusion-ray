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
    exchangers: int,
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

    local_config = SessionConfig()
    local_config.set("datafusion.execution.target_partitions", f"{concurrency}")

    local_ctx = SessionContext(local_config)

    for table in table_names:
        path = os.path.join(data_path, f"{table}.parquet")
        print(f"Registering table {table} using path {path}")
        if listing_tables:
            local_ctx.register_listing_table(table, f"{path}/")
        else:
            local_ctx.register_parquet(table, path)

    results = {
        "engine": "datafusion-python",
        "benchmark": "tpch",
        "data_path": data_path,
        "queries": {},
    }
    results["local_queries"] = {}
    results["validated"] = {}

    duckdb.sql("load tpch")

    queries = range(1, 23) if qnum == -1 else [qnum]
    for qnum in queries:
        print("Running query ", qnum)
        sql: str = duckdb.sql(
            f"select * from tpch_queries() where query_nr=?", params=(qnum,)
        ).df()["query"][0]

        print("executing ", sql)

        start_time = time.time()
        df = local_ctx.sql("explain analyze " + sql)
        end_time = time.time()
        part1 = end_time - start_time
        print("=== Physical Plan ===")
        print(df.execution_plan().display_indent())

        start_time = time.time()
        # df.collect()
        df.explain(False, True)
        end_time = time.time()
        results["local_queries"][qnum] = end_time - start_time

    results = json.dumps(results, indent=4)
    current_time_millis = int(datetime.now().timestamp() * 1000)
    results_path = f"datafusion-ray-tpch-{current_time_millis}.json"
    print(f"Writing results to {results_path}")
    # with open(results_path, "w") as f:
    #    f.write(results)

    # write results to stdout
    print(results)


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
        "--exchangers", type=int, default=1, help="Number of Exchange Actors"
    )
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
        args.exchangers,
    )
