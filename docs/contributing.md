<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# DataFusion Ray Contributor Guide

## Building

To build DataFusion Ray, you will need rust installed, as well as [https://github.com/PyO3/maturin](maturin).

Install maturin in your current python environment (a virtual environment is recommended), with

```bash
pip install maturin
```

Then build the project with the following command:

```bash
maturin develop # --release for a release build
```

## Example

- In the `examples` directory, run

```bash
RAY_COLOR_PREFIX=1 RAY_DEDUP_LOGS=0 python tips.py --data-dir=$(pwd)/../testdata/tips/
```

- In the `tpch` directory, use `make_data.py` to create a TPCH dataset at a provided scale factor, then

```bash
RAY_COLOR_PREFIX=1 RAY_DEDUP_LOGS=0 python tpc.py --data=file:///path/to/your/tpch/directory/ --concurrency=2 --batch-size=8182 --worker-pool-min=10 --qnum 2
```

To execute the TPCH query #2. To execute an arbitrary query against the TPCH dataset, provide it with `--query` instead of `--qnum`. This is useful for validating plans that DataFusion Ray will create.

For example, to execute the following query:

```bash
RAY_COLOR_PREFIX=1 RAY_DEDUP_LOGS=0 python tpc.py --data=file:///path/to/your/tpch/directory/ --concurrency=2 --batch-size=8182 --worker-pool-min=10 --query 'select c.c_name, sum(o.o_totalprice) as total from orders o inner join customer c on o.o_custkey = c.c_custkey group by c_name limit 1'
```

To further parallelize execution, you can choose how many partitions will be served by each Stage with `--partitions-per-worker`. If this number is less than `--concurrency` Then multiple Actors will host portions of the stage. For example, if there are 10 stages calculated for a query, `concurrency=16` and `partitions-per-worker=4`, then `40` `RayStage` Actors will be created. If `partitions-per-worker=16` or is absent, then `10` `RayStage` Actors will be created.

To validate the output against non-ray single node datafusion, add `--validate` which will ensure that both systems produce the same output.

To run the entire TPCH benchmark use

```bash
RAY_COLOR_PREFIX=1 RAY_DEDUP_LOGS=0 python tpcbench.py --data=file:///path/to/your/tpch/directory/ --concurrency=2 --batch-size=8182 --worker-pool-min=10 [--partitions-per-worker=] [--validate]
```

This will output a json file in the current directory with query timings.

## Logging

DataFusion Ray's logging output is determined by the `DATAFUSION_RAY_LOG_LEVEL` environment variable. The default log level is `WARN`. To change the log level, set the environment variable to one of the following values: `ERROR`, `WARN`, `INFO`, `DEBUG`, or `TRACE`.

DataFusion Ray outputs logs from both python and rust, and in order to handle this consistently, the python logger for `datafusion_ray` is routed to rust for logging. The `RUST_LOG` environment variable can be used to control other rust log output other than `datafusion_ray`.

## Status

- DataFusion Ray can execute all TPCH queries. Tested up to SF100.

## Known Issues

- We are waiting to upgrade to a DataFusion version where the parquet options are serialized into substrait in order to send them correctly in a plan. Currently, we
  manually add back `table_parquet_options.pushdown_filters=true` after deserialization to compensate. This will be refactored in the future.

see <https://github.com/apache/datafusion/pull/14465>
