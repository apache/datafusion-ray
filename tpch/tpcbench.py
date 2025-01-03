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
from datafusion_ray import DataFusionRayContext
from datetime import datetime
import pyarrow as pa
import json
import time


def main(benchmark: str, data_path: str, query_path: str, concurrency: int):

    # Register the tables
    if benchmark == "tpch":
        num_queries = 22
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
    elif benchmark == "tpcds":
        num_queries = 99
        table_names = [
            "call_center",
            "catalog_page",
            "catalog_returns",
            "catalog_sales",
            "customer",
            "customer_address",
            "customer_demographics",
            "date_dim",
            "time_dim",
            "household_demographics",
            "income_band",
            "inventory",
            "item",
            "promotion",
            "reason",
            "ship_mode",
            "store",
            "store_returns",
            "store_sales",
            "warehouse",
            "web_page",
            "web_returns",
            "web_sales",
            "web_site",
        ]
    else:
        raise Exception("invalid benchmark")

    # Connect to a cluster
    # use ray job submit
    ray.init()

    ctx = DataFusionRayContext()
    ctx.set("datafusion.execution.parquet.pushdown_filters", "true")
    ctx.set("datafusion.execution.target_partitions", f"{concurrency}")
    ctx.set("datafusion.optimizer.enable_round_robin_repartition", "false")

    for table in table_names:
        path = f"{data_path}/{table}.parquet"
        print(f"Registering table {table} using path {path}")
        ctx.register_parquet(table, path)

    results = {
        "engine": "datafusion-python",
        "benchmark": benchmark,
        "data_path": data_path,
        "query_path": query_path,
        "queries": {},
    }

    # for query in range(1, num_queries + 1):
    for query in [3]:

        # read text file
        path = f"{query_path}/q{query}.sql"
        print(f"Reading query {query} using path {path}")
        with open(path, "r") as f:
            text = f.read()
            # each file can contain multiple queries
            queries = text.split(";")

            start_time = time.time()
            for sql in queries:
                sql = sql.strip()
                if len(sql) > 0:
                    print(f"Executing: {sql}")
                    batches = ctx.sql(sql).collect()
                    table = pa.Table.from_batches(batches)

                    print(f"Query {query} returned {len(table)} rows")
            end_time = time.time()
            print(f"Query {query} took {end_time - start_time} seconds")

            # store timings in list and later add option to run > 1 iterations
            results["queries"][query] = [end_time - start_time]

    str = json.dumps(results, indent=4)
    current_time_millis = int(datetime.now().timestamp() * 1000)
    results_path = f"datafusion-ray-{benchmark}-{current_time_millis}.json"
    print(f"Writing results to {results_path}")
    with open(results_path, "w") as f:
        f.write(str)

    # write results to stdout
    print(str)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="DataFusion benchmark derived from TPC-H / TPC-DS"
    )
    parser.add_argument(
        "--benchmark", required=True, help="Benchmark to run (tpch or tpcds)"
    )
    parser.add_argument("--data", required=True, help="Path to data files")
    parser.add_argument("--queries", required=True, help="Path to query files")
    parser.add_argument(
        "--concurrency", required=True, help="Number of concurrent tasks"
    )
    args = parser.parse_args()

    main(args.benchmark, args.data, args.queries, int(args.concurrency))

