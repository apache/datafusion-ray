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

## Overview

DataFusion Ray is a distributed execution framework that enables DataFusion DataFrame and SQL queries to run on a
Ray cluster. This integration allows users to leverage Ray's dynamic scheduling capabilities while executing
queries in a distributed fashion.

## Execution Modes

DataFusion Ray supports two execution modes:

### Streaming Execution

This mode mimics the default execution strategy of DataFusion. Each operator in the query plan starts executing
as soon as its inputs are available, leading to a more pipelined execution model.

**Pros:**

- Lower latency for streaming-like queries where intermediate results can be processed as they arrive.
- Efficient for smaller queries that fit within a single Ray task's memory.
- Good for interactive workloads.

**Cons:**

- Can lead to high memory pressure as intermediate results are held in memory instead of being written to disk.
- More difficult to scale efficiently for large queries since tasks must hold data until downstream consumers are ready.

### Batch Execution (not implemented yet)

In this mode, execution follows a staged model similar to Apache Spark. Each query stage runs to completion, producing 
intermediate shuffle files that are persisted and used as input for the next stage.

**Pros:**

- Better memory management since intermediate results are persisted instead of held in memory.
- Improved scalability for large queries due to controlled task execution and shuffle handling.
- More predictable execution patterns suitable for batch processing.

**Cons:**

- Higher query latency due to the overhead of writing and reading shuffle files.
- May not be as efficient for interactive or low-latency queries.

## Getting Started

See the [contributor guide] for instructions on building DataFusion Ray.

Once installed, you can run queries using DataFusion's familiar API while leveraging the distributed execution 
capabilities of Ray.

```python
import ray
from datafusion_ray import DFRayContext

ray.init()
session = DFRayContext()
df = session.sql("SELECT * FROM my_table WHERE value > 100")
df.show()
```

## Use Cases

Greedy Execution Mode is ideal for interactive queries and real-time analytics with lower latency requirements.

Spark-like Execution Mode is better suited for batch processing, large-scale queries, and workloads where memory
constraints are a concern.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request if you would like to contribute. See the
[contributor guide] for more information.

## License

DataFusion Ray is licensed under Apache 2.0.


[contributor guide]: docs/contributing.md