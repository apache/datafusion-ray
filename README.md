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

> This was originally a research project donated from [ray-sql] to evaluate performing distributed SQL queries from
> Python, using [Ray] and [Apache DataFusion]

[ray-sql]: https://github.com/datafusion-contrib/ray-sql

DataFusion Ray is a distributed Python DataFrame and SQL query engine powered by the Rust implementation
of [Apache Arrow], [Apache DataFusion], and [Ray].

[Ray]: https://www.ray.io/
[Apache Arrow]: https://arrow.apache.org/
[Apache DataFusion]: https://datafusion.apache.org/

## Comparison to other DataFusion projects

### Comparison to DataFusion Ballista

- Unlike [DataFusion Ballista], DataFusion Ray does not provide its own distributed scheduler and instead relies on
  Ray for this functionality. As a result of this design choice, DataFusion Ray is a much smaller and simpler project.
- DataFusion Ray is Python-first, and DataFusion Ballista is Rust-first

[DataFusion Ballista]: https://github.com/apache/datafusion-ballista

### Comparison to DataFusion Python

- [DataFusion Python] provides a Python DataFrame and SQL API for in-process execution. DataFusion Ray extends
  DataFusion Python to provide scalability across multiple nodes.

[DataFusion Python]: https://github.com/apache/datafusion-python

## Example

## Status

- This branch is a work in progress refactoring datafusion-ray and using arrow flight for streaming batches between partitions during execution.
