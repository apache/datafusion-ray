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
from datafusion import SessionContext, SessionConfig, RuntimeConfig, col, lit, functions as F
import pytest

@pytest.fixture
def df_ctx():
    """Fixture to create a DataFusion context."""
    # used fixed partition count so that tests are deterministic on different environments
    config = SessionConfig().with_target_partitions(4)
    return SessionContext(config=config)

@pytest.fixture
def ctx(df_ctx):
    """Fixture to create a Datafusion Ray context."""
    return DatafusionRayContext(df_ctx)

def test_basic_query_succeed(df_ctx, ctx):
    df_ctx.register_csv("tips", "examples/tips.csv", has_header=True)
    record_batches = ctx.sql("SELECT * FROM tips")
    assert len(record_batches) <= 4
    num_rows = sum(batch.num_rows for batch in record_batches)
    assert num_rows == 244

def test_aggregate_csv(df_ctx, ctx):
    df_ctx.register_csv("tips", "examples/tips.csv", has_header=True)
    record_batches = ctx.sql("select sex, smoker, avg(tip/total_bill) as tip_pct from tips group by sex, smoker")
    assert len(record_batches) <= 4
    num_rows = sum(batch.num_rows for batch in record_batches)
    assert num_rows == 4

def test_aggregate_parquet(df_ctx, ctx):
    df_ctx.register_parquet("tips", "examples/tips.parquet")
    record_batches = ctx.sql("select sex, smoker, avg(tip/total_bill) as tip_pct from tips group by sex, smoker")
    assert len(record_batches) <= 4
    num_rows = sum(batch.num_rows for batch in record_batches)
    assert num_rows == 4

def test_aggregate_parquet_dataframe(df_ctx, ctx):
    df = df_ctx.read_parquet(f"examples/tips.parquet")
    df = (
        df.aggregate(
            [col("sex"), col("smoker"), col("day"), col("time")],
            [F.avg(col("tip") / col("total_bill")).alias("tip_pct")],
        )
        .filter(col("day") != lit("Dinner"))
        .aggregate([col("sex"), col("smoker")], [F.avg(col("tip_pct")).alias("avg_pct")])
    )
    ray_results = ctx.plan(df.execution_plan())
    df_ctx.create_dataframe([ray_results]).show()


def test_no_result_query(df_ctx, ctx):
    df_ctx.register_csv("tips", "examples/tips.csv", has_header=True)
    ctx.sql("CREATE VIEW tips_view AS SELECT * FROM tips")
