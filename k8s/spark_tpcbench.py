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
from datetime import datetime
import json
from pyspark.sql import SparkSession
import time
import sys


def main(benchmark: str, data_path: str, query_path: str, output_path: str, name: str):

    # Initialize a SparkSession
    spark = SparkSession.builder \
        .appName( f"{name} benchmark derived from {benchmark}") \
        .getOrCreate()

    spark.conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        # Register the tables
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

    for table in table_names:
        path = f"{data_path}/{table}.parquet"
        print(f"Registering table {table} using path {path}")
        df = spark.read.parquet(path)
        df.createOrReplaceTempView(table)

    conf_dict = {k: v for k, v in spark.sparkContext.getConf().getAll()}

    results = {
        "engine": "spark",
        "benchmark": benchmark,
        "data_path": data_path,
        "query_path": query_path,
        "spark_conf": conf_dict,
        "queries": {},
    }

    iter_start_time = time.time()

    for query in range(1, num_queries + 1):
        spark.sparkContext.setJobDescription(f"{benchmark} q{query}")

        # if query == 9:
        #     continue

        # read text file
        path = f"{query_path}/q{query}.sql"

        # if query == 72:
        #     # use version with sensible join order
        #     path = f"{query_path}/q{query}_optimized.sql"

        print(f"Reading query {query} using path {path}")
        with open(path, "r") as f:
            text = f.read()
            # each file can contain multiple queries
            queries = list(
                filter(lambda x: len(x) > 0, map(lambda x: x.strip(), text.split(";")))
            )

            start_time = time.time()
            for sql in queries:
                sql = sql.strip().replace("create view", "create temp view")
                if len(sql) > 0:
                    print(f"Executing: {sql}")
                    df = spark.sql(sql)
                    rows = df.collect()
            end_time = time.time()

            out_path = f"{output_path}/{name}_{benchmark}_q{query}_result.txt"
            # fIXME: concat output for all queries.  For example q15 has multiple
            out = df._show_string(100000)
            with open(out_path, "w") as f:
                f.write(out)

            print(f"Query {query} took {end_time - start_time} seconds")

            results["queries"][str(query)] = end_time - start_time
            print(json.dumps(results, indent=4))

    iter_end_time = time.time()
    print(f"total took {round(iter_end_time - iter_start_time,2)} seconds")

    out = json.dumps(results, indent=4)
    current_time_millis = int(datetime.now().timestamp() * 1000)
    results_path = f"{output_path}/{name}-{benchmark}-{current_time_millis}.json"
    print(f"Writing results to {results_path}")
    with open(results_path, "w") as f:
        f.write(out)

    # Stop the SparkSession
    spark.stop()


if __name__ == "__main__":
    print(f"got arguments {sys.argv}")
    print(f"python version {sys.version}")
    print(f"python versioninfo  {sys.version_info}")

    parser = argparse.ArgumentParser(
        description="DataFusion benchmark derived from TPC-H / TPC-DS"
    )
    parser.add_argument(
        "--benchmark", required=True, help="Benchmark to run (tpch or tpcds)"
    )
    parser.add_argument("--data", required=True, help="Path to data files")
    parser.add_argument("--queries", required=True, help="Path to query files")
    parser.add_argument("--output", required=True, help="Path to write output")
    parser.add_argument(
        "--name", required=True, help="Prefix for result file e.g. spark/comet/gluten"
    )
    args = parser.parse_args()
    print(f"parsed is {args}")

    main(args.benchmark, args.data, args.queries, args.output, args.name)
