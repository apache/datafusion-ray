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
import glob
import json
import time
import os
import ray
import pyarrow as pa
from datafusion_ray import RayContext


def go(data_dir: str, concurrency: int, isolate: bool):
    print(f"isolate {isolate}")
    ctx = RayContext(isolate_partitions=isolate, bucket="rob-tandy-tmp")
    ctx.set("datafusion.execution.target_partitions", str(concurrency))
    ctx.set("datafusion.catalog.information_schema", "true")
    ctx.set("datafusion.optimizer.enable_round_robin_repartition", "false")

    for table in [
        "customer",
        "orders",
    ]:
        f = os.path.join(data_dir, f"{table}.parquet")
        print("Registering table", table, "using path", f)
        ctx.register_parquet(table, f)

    query = """SELECT customer.c_name, sum(orders.o_totalprice) as total_amount
    FROM customer JOIN orders ON customer.c_custkey = orders.o_custkey
    GROUP BY customer.c_name order by total_amount desc limit 10"""

    # query = """SELECT count(customer.c_name), customer.c_mktsegment from customer group by customer.c_mktsegment limit 10"""

    df = ctx.sql(query)
    print(df.execution_plan().display_indent())
    for stage in df.stages():
        print(f"Stage ", stage.stage_id)
        print(stage.execution_plan().display_indent())

    df.show()

    time.sleep(3)
    print(json.dumps(df.totals(), indent=4))


if __name__ == "__main__":
    ray.init(namespace="example")
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=True, help="path to tpch*.parquet files")
    parser.add_argument("--concurrency", required=True, type=int)
    parser.add_argument(
        "--isolate",
        action="store_true",
        help="do each partition as a separate ray actor, more concurrency",
    )
    args = parser.parse_args()

    go(args.data, args.concurrency, args.isolate)
