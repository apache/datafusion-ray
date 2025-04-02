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

# DataFusion for Ray

[![Apache licensed][license-badge]][license-url]
[![Python Tests][actions-badge]][actions-url]
[![Discord chat][discord-badge]][discord-url]

[license-badge]: https://img.shields.io/badge/license-Apache%20v2-blue.svg
[license-url]: https://github.com/apache/datafusion-ray/blob/main/LICENSE.txt
[actions-badge]: https://github.com/apache/datafusion-ray/actions/workflows/main.yml/badge.svg
[actions-url]: https://github.com/apache/datafusion-ray/actions?query=branch%3Amain
[discord-badge]: https://img.shields.io/badge/Chat-Discord-purple
[discord-url]: https://discord.com/invite/Qw5gKqHxUM

## Overview

DataFusion for Ray is a distributed execution framework that enables DataFusion DataFrame and SQL queries to run on a
Ray cluster. This integration allows users to leverage Ray's dynamic scheduling capabilities while executing
queries in a distributed fashion.

## Execution Modes

DataFusion for Ray supports two execution modes:

### Streaming Execution

This mode mimics the default execution strategy of DataFusion. Each operator in the query plan starts executing
as soon as its inputs are available, leading to a more pipelined execution model.

### Batch Execution

_Note: Batch Execution is not implemented yet. Tracking issue: <https://github.com/apache/datafusion-ray/issues/69>_

In this mode, execution follows a staged model similar to Apache Spark. Each query stage runs to completion, producing
intermediate shuffle files that are persisted and used as input for the next stage.

## Getting Started

See the [contributor guide] for instructions on building DataFusion for Ray.

Once installed, you can run queries using DataFusion's familiar API while leveraging the distributed execution
capabilities of Ray.

```python
# from example in ./examples/http_csv.py
import ray
from datafusion_ray import DFRayContext, df_ray_runtime_env

ray.init(runtime_env=df_ray_runtime_env)

ctx = DFRayContext()
ctx.register_csv(
    "aggregate_test_100",
    "https://github.com/apache/arrow-testing/raw/master/data/csv/aggregate_test_100.csv",
)

df = ctx.sql("SELECT c1,c2,c3 FROM aggregate_test_100 LIMIT 5")

df.show()
```

## Contributing

Contributions are welcome! Please open an issue or submit a pull request if you would like to contribute. See the
[contributor guide] for more information.

## License

DataFusion for Ray is licensed under Apache 2.0.

[contributor guide]: docs/contributing.md
