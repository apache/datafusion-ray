#!/bin/bash

set -e
# Create necessary directories
mkdir -p data
mkdir -p test_files/tpch/data

# Check if the data folder is empty
if [ -z "$(ls -A data)" ]; then
  echo "Data folder is empty. Cloning repository..."
  git clone https://github.com/databricks/tpch-dbgen.git tpch/tpch-dbgen
  cd tpch/tpch-dbgen
  make
  # consistent with DataFusion test strategy
  ./dbgen -f -s "$TPCH_SAMPLING_RATE"
  pwd
  ls
  mv ./*.tbl ../../data
  cd ../../
  pwd
  python -m tpch.tpchgen convert --partitions "$TPCH_TEST_PARTITIONS"
else
  echo "Data folder is not empty. Skipping cloning and data generation."
fi
