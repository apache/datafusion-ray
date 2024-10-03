#!/bin/bash

set -e
mkdir -p data
python -m tpch.tpchgen generate --scale-factor "$TPCH_SAMPLING_RATE" --partitions "$TPCH_TEST_PARTITIONS"
python -m tpch.tpchgen convert --partitions "$TPCH_TEST_PARTITIONS"
