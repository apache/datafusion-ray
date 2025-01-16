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
from datafusion_ray import RayContext
from datetime import datetime
import pyarrow as pa
import json
import os
import time

import duckdb
from datafusion.object_store import AmazonS3


def main(data_path: str, concurrency: int, batch_size: int, isolate_partitions: bool):

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
    ctx.set("datafusion.execution.parquet.pushdown_filters", "true")
    ctx.set("datafusion.optimizer.enable_round_robin_repartition", "false")

    for table in table_names:
        path = f"{data_path}/{table}.parquet"
        print(f"Registering table {table} using path {path}")
        ctx.register_parquet(table, path)

    results = {
        "engine": "datafusion-python",
        "benchmark": "tpch",
        "data_path": data_path,
        "queries": {},
    }

    duckdb.sql("load tpch")

    # for query in range(1, num_queries + 1):
    #
    # for qnum in range(1, 23):
    for qnum in [1, 2, 3, 4, 5]:
        sql: str = duckdb.sql(
            f"select * from tpch_queries() where query_nr=?", params=(qnum,)
        ).df()["query"][0]
        print("executing ", sql)

        start_time = time.time()
        df = ctx.sql(sql)
        for stage in df.stages():
            print("Stage ", stage.stage_id)
            print(stage.execution_plan().display_indent())

        batches = df.collect()
        table = pa.Table.from_batches(batches)
        end_time = time.time()
        df.show()
        size = sum([batch.get_total_buffer_size() for batch in batches])
        print(
            f"testQuery {qnum} took {end_time - start_time} seconds, {len(batches)} batches, result size {size}"
        )
        results["queries"][qnum] = [end_time - start_time]

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
    parser.add_argument(
        "--batch-size",
        required=False,
        default=8192,
        help="Desired batch size output per stage",
    )
    args = parser.parse_args()

    main(args.data, int(args.concurrency), int(args.batch_size), args.isolate)
