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

from datafusion_ray.context import DatafusionRayContext
from datafusion import SessionContext
import pytest


def test_basic_query_succeed():
    df_ctx = SessionContext()
    ctx = DatafusionRayContext(df_ctx)
    df_ctx.register_csv("tips", "examples/tips.csv", has_header=True)
    record_batch = ctx.sql("SELECT * FROM tips")
    assert record_batch.num_rows == 244


def test_no_result_query():
    df_ctx = SessionContext()
    ctx = DatafusionRayContext(df_ctx)
    df_ctx.register_csv("tips", "examples/tips.csv", has_header=True)
    ctx.sql("CREATE VIEW tips_view AS SELECT * FROM tips")
