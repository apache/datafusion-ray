#!/bin/bash

set -e

create_directories() {
  mkdir -p data
}

clone_and_build_tpch_dbgen() {
  if [ -z "$(ls -A tpch/tpch-dbgen)" ]; then
    echo "tpch/tpch-dbgen folder is empty. Cloning repository..."
    git clone https://github.com/databricks/tpch-dbgen.git tpch/tpch-dbgen
    cd tpch/tpch-dbgen
    make
    cd ../../
  else
    echo "tpch/tpch-dbgen folder is not empty. Skipping cloning of TPCH dbgen."
  fi
}

generate_data() {
  cd tpch/tpch-dbgen
  if [ "$TPCH_TEST_PARTITIONS" -gt 1 ]; then
    for i in $(seq 1 "$TPCH_TEST_PARTITIONS"); do
      ./dbgen -f -s "$TPCH_SCALING_FACTOR" -C "$TPCH_TEST_PARTITIONS" -S "$i"
    done
  else
    ./dbgen -f -s "$TPCH_SCALING_FACTOR"
  fi
  mv ./*.tbl* ../../data
}

convert_data() {
  cd ../../
  python -m tpch.tpchgen convert --partitions "$TPCH_TEST_PARTITIONS"
}

main() {
  if [ -z "$TPCH_TEST_PARTITIONS" ]; then
    echo "Error: TPCH_TEST_PARTITIONS is not set."
    exit 1
  fi

  if [ -z "$TPCH_SCALING_FACTOR" ]; then
    echo "Error: TPCH_SCALING_FACTOR is not set."
    exit 1
  fi

  create_directories

  if [ -z "$(ls -A data)" ]; then
    clone_and_build_tpch_dbgen
    generate_data
    convert_data
  else
    echo "Data folder is not empty. Skipping cloning and data generation."
  fi
}

main
