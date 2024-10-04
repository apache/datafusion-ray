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

# DataFusion on Ray

> This was originally a research project donated from [ray-sql](https://github.com/datafusion-contrib/ray-sql) to evaluate performing distributed SQL queries from Python, using
> [Ray](https://www.ray.io/) and [DataFusion](https://github.com/apache/arrow-datafusion).

DataFusion Ray is a distributed SQL query engine powered by the Rust implementation of [Apache Arrow](https://arrow.apache.org/), [Apache DataFusion](https://datafusion.apache.org/) and [Ray](https://www.ray.io/).

## Goals

- Demonstrate how easily new systems can be built on top of DataFusion. See the [design documentation](./docs/README.md)
  to understand how RaySQL works.
- Drive requirements for DataFusion's [Python bindings](https://github.com/apache/arrow-datafusion-python).
- Create content for an interesting blog post or conference talk.

## Non Goals

- Re-build the cluster scheduling systems like what [Ballista](https://datafusion.apache.org/ballista/) did.
  - Ballista is extremely complex and utilizing Ray feels like it abstracts some of that complexity away.
  - Datafusion Ray is delegating cluster management to Ray.

## Example

Run the following example live in your browser using a Google Colab [notebook](https://colab.research.google.com/drive/1tmSX0Lu6UFh58_-DBUVoyYx6BoXHOszP?usp=sharing).

```python
import os
import pandas as pd
import ray

from datafusion_ray import RaySqlContext

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

# Start a local cluster
ray.init(resources={"worker": 1})

# Create a context and register a table
ctx = RaySqlContext(2, use_ray_shuffle=True)
# Register either a CSV or Parquet file
# ctx.register_csv("tips", f"{SCRIPT_DIR}/tips.csv", True)
ctx.register_parquet("tips", f"{SCRIPT_DIR}/tips.parquet")

result_set = ctx.sql(
  "select sex, smoker, avg(tip/total_bill) as tip_pct from tips group by sex, smoker"
)
for record_batch in result_set:
  print(record_batch.to_pandas())
```

## Status

- DataFusion Ray can run all queries in the TPC-H benchmark

## Features

- Mature SQL support (CTEs, joins, subqueries, etc) thanks to DataFusion
- Support for CSV and Parquet files

## Limitations

- Requires a shared file system currently. Check details [here](./docs/README.md#distributed-shuffle).

## Performance

This chart shows the performance of DataFusion Ray compared to Apache Spark for
[SQLBench-H](https://sqlbenchmarks.io/sqlbench-h/) at a very small data set (10GB), running on a desktop (Threadripper
with 24 physical cores). Both DataFusion Ray and Spark are configured with 24 executors.

### Overall Time

DataFusion Ray is ~1.9x faster overall for this scale factor and environment with disk-based shuffle.

![SQLBench-H Total](./docs/sqlbench-h-total.png)

### Per Query Time

Spark is much faster on some queries, likely due to broadcast exchanges, which DataFusion Ray hasn't implemented yet.

![SQLBench-H Per Query](./docs/sqlbench-h-per-query.png)

### Performance Plan

Plans on experimenting with the following changes to improve performance:

- Make better use of Ray futures to run more tasks in parallel
- Use Ray object store for shuffle data transfer to reduce disk I/O cost
- Keep upgrading to newer versions of DataFusion to pick up the latest optimizations

## Building

```bash
# prepare development environment (used to build wheel / install in development)
python3 -m venv venv
# activate the venv
source venv/bin/activate
# update pip itself if necessary
python -m pip install -U pip
# install dependencies (for Python 3.8+)
python -m pip install -r requirements-in.txt
```

Whenever rust code changes (your changes or via `git pull`):

````bash
# make sure you activate the venv using "source venv/bin/activate" first
maturin develop python -m pytest ```


## Testing

Running local Rust tests require generating the tpch-data. This can be done
by running the following command:

```bash
./scripts/generate_tpch_data.sh
```

Tests compare plans with expected plans, which unfortunately contain the
path to the parquet tables. The path committed under version control is
the one of a Github Runner, and won't work locally. You can fix it by
running the following command:

```bash
./scripts/replace-expected-plan-paths.sh local-dev
````

When instead you need to regenerate the plans, which you can do by
re-running the planner tests removing all the content of
`testdata/expected-plans`, they will now contain your local paths. You can
fix it before committing the plans running

```bash

./scripts/replace-expected-plan-paths.sh pre-ci

```

## Benchmarking

Create a release build when running benchmarks, then use pip to install the wheel.

```bash
maturin develop --release
```

## How to update dependencies

To change test dependencies, change the `requirements.in` and run

```bash
# install pip-tools (this can be done only once), also consider running in venv
python -m pip install pip-tools
python -m piptools compile --generate-hashes -o requirements-310.txt
```

To update dependencies, run with `-U`

```bash
python -m piptools compile -U --generate-hashes -o requirements-310.txt
```

More details [here](https://github.com/jazzband/pip-tools)
