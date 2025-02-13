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

import duckdb
from datafusion.object_store import AmazonS3


def make_ctx(
    data_path: str,
    concurrency: int,
    batch_size: int,
    partitions_per_worker: int | None,
    listing_tables: bool,
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

    ctx = RayContext(batch_size=batch_size, partitions_per_worker=partitions_per_worker)

    ctx.set("datafusion.execution.target_partitions", f"{concurrency}")
    # ctx.set("datafusion.execution.parquet.pushdown_filters", "true")
    ctx.set("datafusion.optimizer.enable_round_robin_repartition", "false")
    ctx.set("datafusion.execution.coalesce_batches", "false")

    for table in table_names:
        path = os.path.join(data_path, f"{table}.parquet")
        print(f"Registering table {table} using path {path}")
        if listing_tables:
            ctx.register_listing_table(table, f"{path}/")
        else:
            ctx.register_parquet(table, path)

    return ctx


def main(
    data_path: str,
    concurrency: int,
    batch_size: int,
    query: str,
    partitions_per_worker: int | None,
    validate: bool,
    listing_tables,
) -> None:
    ctx = make_ctx(
        data_path, concurrency, batch_size, partitions_per_worker, listing_tables
    )
    df = ctx.sql(query)
    for stage in df.stages():
        print(
            f"Stage {stage.stage_id} output partitions:{stage.num_output_partitions} partition_groups: {stage.partition_groups}"
        )
        print(stage.execution_plan().display_indent())

    df.show()


def tpch_query(qnum: int) -> str:
    query_path = os.path.join(os.path.dirname(__file__), "..", "testdata", "queries")
    return open(os.path.join(query_path, f"q{qnum}.sql")).read()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=str, help="data path")
    parser.add_argument("--query", type=str, help="query")
    parser.add_argument(
        "--qnum", type=int, default=0, help="query number for TPCH benchmark"
    )
    parser.add_argument("--concurrency", type=int, help="concurrency")
    parser.add_argument("--batch-size", type=int, help="batch size")
    parser.add_argument(
        "--partitions-per-worker",
        type=int,
        help="Max partitions per Stage Service Worker",
    )
    parser.add_argument("--validate", action="store_true")
    parser.add_argument("--listing-tables", action="store_true")
    args = parser.parse_args()

    if args.qnum > 0:
        query = tpch_query(int(args.qnum))
    else:
        query = args.query

    main(
        args.data,
        int(args.concurrency),
        int(args.batch_size),
        query,
        args.partitions_per_worker,
        args.validate,
        args.listing_tables,
    )
