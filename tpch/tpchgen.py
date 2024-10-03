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
import concurrent.futures
from datafusion import SessionContext
import os
import pyarrow
import subprocess
import time

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

# schema definition copied from DataFusion Python tpch example
all_schemas = {}

all_schemas["customer"] = [
    ("C_CUSTKEY", pyarrow.int64()),
    ("C_NAME", pyarrow.string()),
    ("C_ADDRESS", pyarrow.string()),
    ("C_NATIONKEY", pyarrow.int64()),
    ("C_PHONE", pyarrow.string()),
    ("C_ACCTBAL", pyarrow.decimal128(11, 2)),
    ("C_MKTSEGMENT", pyarrow.string()),
    ("C_COMMENT", pyarrow.string()),
]

all_schemas["lineitem"] = [
    ("L_ORDERKEY", pyarrow.int64()),
    ("L_PARTKEY", pyarrow.int64()),
    ("L_SUPPKEY", pyarrow.int64()),
    ("L_LINENUMBER", pyarrow.int32()),
    ("L_QUANTITY", pyarrow.decimal128(11, 2)),
    ("L_EXTENDEDPRICE", pyarrow.decimal128(11, 2)),
    ("L_DISCOUNT", pyarrow.decimal128(11, 2)),
    ("L_TAX", pyarrow.decimal128(11, 2)),
    ("L_RETURNFLAG", pyarrow.string()),
    ("L_LINESTATUS", pyarrow.string()),
    ("L_SHIPDATE", pyarrow.date32()),
    ("L_COMMITDATE", pyarrow.date32()),
    ("L_RECEIPTDATE", pyarrow.date32()),
    ("L_SHIPINSTRUCT", pyarrow.string()),
    ("L_SHIPMODE", pyarrow.string()),
    ("L_COMMENT", pyarrow.string()),
]

all_schemas["nation"] = [
    ("N_NATIONKEY", pyarrow.int64()),
    ("N_NAME", pyarrow.string()),
    ("N_REGIONKEY", pyarrow.int64()),
    ("N_COMMENT", pyarrow.string()),
]

all_schemas["orders"] = [
    ("O_ORDERKEY", pyarrow.int64()),
    ("O_CUSTKEY", pyarrow.int64()),
    ("O_ORDERSTATUS", pyarrow.string()),
    ("O_TOTALPRICE", pyarrow.decimal128(11, 2)),
    ("O_ORDERDATE", pyarrow.date32()),
    ("O_ORDERPRIORITY", pyarrow.string()),
    ("O_CLERK", pyarrow.string()),
    ("O_SHIPPRIORITY", pyarrow.int32()),
    ("O_COMMENT", pyarrow.string()),
]

all_schemas["part"] = [
    ("P_PARTKEY", pyarrow.int64()),
    ("P_NAME", pyarrow.string()),
    ("P_MFGR", pyarrow.string()),
    ("P_BRAND", pyarrow.string()),
    ("P_TYPE", pyarrow.string()),
    ("P_SIZE", pyarrow.int32()),
    ("P_CONTAINER", pyarrow.string()),
    ("P_RETAILPRICE", pyarrow.decimal128(11, 2)),
    ("P_COMMENT", pyarrow.string()),
]

all_schemas["partsupp"] = [
    ("PS_PARTKEY", pyarrow.int64()),
    ("PS_SUPPKEY", pyarrow.int64()),
    ("PS_AVAILQTY", pyarrow.int32()),
    ("PS_SUPPLYCOST", pyarrow.decimal128(11, 2)),
    ("PS_COMMENT", pyarrow.string()),
]

all_schemas["region"] = [
    ("R_REGIONKEY", pyarrow.int64()),
    ("R_NAME", pyarrow.string()),
    ("R_COMMENT", pyarrow.string()),
]

all_schemas["supplier"] = [
    ("S_SUPPKEY", pyarrow.int64()),
    ("S_NAME", pyarrow.string()),
    ("S_ADDRESS", pyarrow.string()),
    ("S_NATIONKEY", pyarrow.int64()),
    ("S_PHONE", pyarrow.string()),
    ("S_ACCTBAL", pyarrow.decimal128(11, 2)),
    ("S_COMMENT", pyarrow.string()),
]


def run(cmd: str):
    print(f"Executing: {cmd}")
    subprocess.run(cmd, shell=True, check=True)


def run_and_log_output(cmd: str, log_file: str):
    print(f"Executing: {cmd}; writing output to {log_file}")
    with open(log_file, "w") as file:
        subprocess.run(
            cmd, shell=True, check=True, stdout=file, stderr=subprocess.STDOUT
        )


def convert_tbl_to_parquet(
    ctx: SessionContext,
    table: str,
    tbl_filename: str,
    file_extension: str,
    parquet_filename: str,
):
    print(f"Converting {tbl_filename} to {parquet_filename} ...")

    # schema manipulation code copied from DataFusion Python tpch example
    table_schema = [
        pyarrow.field(r[0].lower(), r[1], nullable=False) for r in all_schemas[table]
    ]

    # Pre-collect the output columns so we can ignore the null field we add
    # in to handle the trailing | in the file
    output_cols = [r.name for r in table_schema]

    # Trailing | requires extra field for in processing
    table_schema.append(pyarrow.field("some_null", pyarrow.null(), nullable=True))

    schema = pyarrow.schema(table_schema)

    df = ctx.read_csv(
        tbl_filename,
        schema=schema,
        has_header=False,
        file_extension=file_extension,
        delimiter="|",
    )
    df = df.select_columns(*output_cols)
    df.write_parquet(parquet_filename, compression="snappy")


def generate_tpch(scale_factor: int, partitions: int):
    start_time = time.time()
    docker_cmd = os.getenv("DOCKER_CMD", "docker")
    if partitions == 1:
        command = f"{docker_cmd} run -v `pwd`/data:/data -t --rm ghcr.io/scalytics/tpch-docker:main -vf -s {scale_factor}"
        run_and_log_output(command, "/tmp/tpchgen.log")
    else:
        max_threads = os.cpu_count()

        # List of commands to run
        commands = [
            (
                f"{docker_cmd} run -v `pwd`/data:/data -t --rm ghcr.io/scalytics/tpch-docker:main -vf -s {scale_factor} -C {partitions} -S {part}",
                f"/tmp/tpchgen-part{part}.log",
            )
            for part in range(1, partitions + 1)
        ]

        # run commands in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
            futures = [
                executor.submit(run_and_log_output, command, log_file)
                for (command, log_file) in commands
            ]

            # wait for all futures to complete
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Command failed with exception: {e}")

    end_time = time.time()
    print(f"Generated CSV data in {round(end_time - start_time, 2)} seconds")


def convert_tpch(partitions: int):
    start_time = time.time()
    ctx = SessionContext()
    if partitions == 1:
        # convert to parquet
        for table in table_names:
            convert_tbl_to_parquet(
                ctx, table, f"data/{table}.tbl", "tbl", f"data/{table}.parquet"
            )
    else:
        for table in table_names:
            run(f"mkdir -p data/{table}.parquet")
            if table == "nation" or table == "region":
                # nation and region are special cases and do not generate multiple files
                convert_tbl_to_parquet(
                    ctx,
                    table,
                    f"data/{table}.tbl",
                    "tbl",
                    f"data/{table}.parquet/part1.parquet",
                )
            else:
                for part in range(1, partitions + 1):
                    convert_tbl_to_parquet(
                        ctx,
                        table,
                        f"data/{table}.tbl.{part}",
                        f"tbl.{part}",
                        f"data/{table}.parquet/part{part}.parquet",
                    )
    end_time = time.time()
    print(f"Converted CSV to Parquet in {round(end_time - start_time, 2)} seconds")


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    subparsers = arg_parser.add_subparsers(dest="command", help="Available commands")

    parser_generate = subparsers.add_parser("generate", help="Generate TPC-H CSV Data")
    parser_generate.add_argument("--scale-factor", type=int, help="The scale factor")
    parser_generate.add_argument(
        "--partitions", type=int, help="The number of partitions"
    )

    parser_convert = subparsers.add_parser(
        "convert", help="Convert TPC-H CSV Data to Parquet"
    )
    parser_convert.add_argument(
        "--partitions", type=int, help="The number of partitions"
    )

    args = arg_parser.parse_args()
    if args.command == "generate":
        generate_tpch(args.scale_factor, args.partitions)
    elif args.command == "convert":
        convert_tpch(args.partitions)
