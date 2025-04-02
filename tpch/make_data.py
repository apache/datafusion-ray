import duckdb

import sys

conn = duckdb.connect()


def make(scale_factor: int, output_path: str):
    statements = [
        "install tpch",
        "load tpch",
        f"call dbgen(sf = {scale_factor})",
    ]
    execute(statements)

    statements = []
    for row in conn.execute("show tables").fetchall():
        table = row[0]
        statements.append(
            f"copy {table} to '{output_path}/{table}.parquet' (format parquet, compression zstd)"
        )
    execute(statements)


def execute(statements):
    for statement in statements:
        print(f"executing: {statement}")
        conn.execute(statement)


if __name__ == "__main__":
    make(int(sys.argv[1]), sys.argv[2])
