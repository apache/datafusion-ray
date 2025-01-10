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
import time

import duckdb


def main(data_path: str, concurrency: int):

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

    ctx = RayContext()
    ctx.set("datafusion.execution.target_partitions", f"{concurrency}")

    local_cfg = SessionConfig()

    local_cfg.set("datafusion.execution.target_partitions", f"{concurrency}")

    local_ctx = SessionContext(local_cfg)

    for table in table_names:
        path = f"{data_path}/{table}.parquet"
        print(f"Registering table {table} using path {path}")
        ctx.register_parquet(table, path)
        local_ctx.register_parquet(table, path)

    results = {
        "engine": "datafusion-python",
        "benchmark": "tpch",
        "data_path": data_path,
        "queries": {},
        "local_queries": {},
    }

    duckdb.sql("load tpch")

    # for query in range(1, num_queries + 1):
    #
    queries = [
        """SELECT customer.c_name, sum(orders.o_totalprice) as total_amount
    FROM customer JOIN orders ON customer.c_custkey = orders.o_custkey
    GROUP BY customer.c_name"""
    ]
    for qnum in [2]:

        # sql: str = duckdb.sql(
        #    f"select * from tpch_queries() where query_nr=?", params=(qnum,)
        # ).df()["query"][0]
        sql = queries[0]

        start_time = time.time()
        df = ctx.sql(sql)
        batches = df.collect()
        table = pa.Table.from_batches(batches)
        end_time = time.time()
        df.show()
        size = sum([batch.get_total_buffer_size() for batch in batches])
        print(
            f"testQuery {qnum} took {end_time - start_time} seconds, {len(batches)} batches, result size {size}"
        )
        results["queries"][qnum] = [end_time - start_time]

        start_time = time.time()
        df = local_ctx.sql(sql)
        batches = df.collect()
        table = pa.Table.from_batches(batches)
        end_time = time.time()
        df.show()

        print(f"Local Query {qnum} took {end_time - start_time} seconds")
        results["local_queries"][qnum] = [end_time - start_time]

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
    args = parser.parse_args()

    main(args.data, int(args.concurrency))
