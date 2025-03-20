import duckdb

import sys
import os
import multiprocessing

conn = duckdb.connect()


def make(scale_factor: int, partitions: int, output_path: str, step: int):
    statements = [
        "install tpch",
        "load tpch",
    ]
    execute(statements)

    print(f"step {step}")
    sql = f"call dbgen(sf={scale_factor}, children={partitions}, step={step})"
    conn.execute(sql)
    conn.sql("show tables").show()

    statements = []

    for row in conn.execute("show tables").fetchall():
        table = row[0]
        os.makedirs(f"{output_path}/{table}.parquet", exist_ok=True)
        statements.append(
            f"copy {table} to '{output_path}/{table}.parquet/part{step}.parquet' (format parquet, compression zstd)"
        )
    execute(statements)


def execute(statements):
    for statement in statements:
        print(f"executing: {statement}")
        conn.execute(statement)


if __name__ == "__main__":
    # this is quick and dirty, it should be tidied up with click to process args
    scale_factor = int(sys.argv[1])
    partitions = int(sys.argv[2])
    data_path = sys.argv[3]
    procs = int(sys.argv[4])

    def go(step):
        make(scale_factor, partitions, data_path, step)

    steps = list(range(partitions))
    with multiprocessing.Pool(processes=procs) as pool:
        pool.map(go, steps)
