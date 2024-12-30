# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import json
import os
import time
from typing import Iterable

import pyarrow as pa
import ray

import datafusion_ray
import datafusion
from typing import List, Any
from datafusion import SessionContext

from datafusion_ray._datafusion_ray_internal import (
    internal_execute_partition,
    make_ray_context,
)


class RayContext:
    def __init__(self):
        self.ctx = make_ray_context(RayShuffler())

    def register_parquet(self, name: str, path: str):
        self.ctx.register_parquet(name, path)

    def register_csv(self, name: str, path: str, has_header: bool):
        self.ctx.register_csv(name, path, has_header)

    def sql(
        self, query: str, options: datafusion.SQLOptions | None = None
    ) -> datafusion.DataFrame:
        if options is None:
            return datafusion.DataFrame(self.ctx.sql(query))
        return datafusion.DataFrame(
            self.ctx.sql_with_options(query, options.options_internal)
        )


class RayShuffler:
    def __init__(self):
        pass

    def execution_partition(self, plan: bytes, partition: int) -> pa.RecordBatchReader:
        print("ray shuffler executing partition")
        ctx = SessionContext()
        return _internal_execute_partition(plan, partition, ctx)
