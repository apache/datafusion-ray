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

You'll need to have both rust and cargo installed.

We will follow the development workflow outlined by [datafusion-python](https://github.com/apache/datafusion-python), [pyo3](https://github.com/PyO3/pyo3) and [maturin](https://github.com/PyO3/maturin).

The Maturin tools used in this workflow can be installed either via `uv` or `pip`. Both approaches should offer the same experience. It is recommended to use `uv` since it has significant performance improvements
over `pip`.

Bootstrap (`uv`):

By default `uv` will attempt to build the datafusion-ray python package. For our development we prefer to build manually. This means
that when creating your virtual environment using `uv sync` you need to pass in the additional `--no-install-package datafusion-ray`. This tells uv, to install all of the dependencies found in `pyproject.toml`, but skip building `datafusion-ray` as we'll do that manually.

```bash
# fetch this repo
git clone git@github.com:apache/datafusion-ray.git
# go to repo root
cd datafusion-ray
# create the virtual enviornment
uv sync --dev --no-install-package datafusion-ray
# activate the environment
source .venv/bin/activate
```

Bootstrap (`pip`):

```bash
# fetch this repo
git clone git@github.com:apache/datafusion-python.git
# go to repo root
cd datafusion-ray
# prepare development environment (used to build wheel / install in development)
python3 -m venv .venv
# activate the venv
source .venv/bin/activate
# update pip itself if necessary
python -m pip install -U pip
# install dependencies
python -m pip install -r pyproject.toml
```

Whenever rust code changes (your changes or via `git pull`):

```bash
# make sure you activate the venv using "source venv/bin/activate" first
maturin develop --uv
python -m pytest
```

## Example

- In the `examples` directory, run

```bash
RAY_COLOR_PREFIX=1 RAY_DEDUP_LOGS=0 python tips.py --data-dir=$(pwd)/../testdata/tips/
```

- In the `tpch` directory, use `make_data.py` to create a TPCH dataset at a provided scale factor, then

```bash
RAY_COLOR_PREFIX=1 RAY_DEDUP_LOGS=0 python tpcbench.py --data=file:///path/to/your/tpch/directory/ --concurrency=2 --batch-size=8182 --worker-pool-min=10 --qnum 2
```

To execute the TPCH query #2. To execute an arbitrary query against the TPCH dataset, provide it with `--query` instead of `--qnum`. This is useful for validating plans that DataFusion Ray will create.

For example, to execute the following query:

```bash
RAY_COLOR_PREFIX=1 RAY_DEDUP_LOGS=0 python tpcbench.py --data=file:///path/to/your/tpch/directory/ --concurrency=2 --batch-size=8182 --worker-pool-min=10 --query 'select c.c_name, sum(o.o_totalprice) as total from orders o inner join customer c on o.o_custkey = c.c_custkey group by c_name limit 1'
```

To further parallelize execution, you can choose how many partitions will be served by each Stage with `--partitions-per-processor`. If this number is less than `--concurrency` Then multiple Actors will host portions of the stage. For example, if there are 10 stages calculated for a query, `concurrency=16` and `partitions-per-processor=4`, then `40` `RayStage` Actors will be created. If `partitions-per-processor=16` or is absent, then `10` `RayStage` Actors will be created.

To validate the output against non-ray single node datafusion, add `--validate` which will ensure that both systems produce the same output.

To run the entire TPCH benchmark use

```bash
RAY_COLOR_PREFIX=1 RAY_DEDUP_LOGS=0 python tpcbench.py --data=file:///path/to/your/tpch/directory/ --concurrency=2 --batch-size=8182 --worker-pool-min=10 [--partitions-per-processor=] [--validate]
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
