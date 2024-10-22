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

import os
import ray

from datafusion import SessionContext, col, lit, functions as F
from datafusion_ray import DatafusionRayContext

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

# Connect to a cluster
ray.init()

# Create a context and register a table
df_ctx = SessionContext()

ray_ctx = DatafusionRayContext(df_ctx)
# Register either a CSV or Parquet file
# ctx.register_csv("tips", f"{SCRIPT_DIR}/tips.csv", True)
df_ctx.register_parquet("tips", f"{SCRIPT_DIR}/tips.parquet")

result_set = ray_ctx.sql(
    "select sex, smoker, avg(tip/total_bill) as tip_pct from tips group by sex, smoker"
)
for record_batch in result_set:
    print(record_batch.to_pandas())

# Alternatively, to use the DataFrame API
df = df_ctx.read_parquet(f"{SCRIPT_DIR}/tips.parquet")
df = (
    df.aggregate(
        [col("sex"), col("smoker"), col("day"), col("time")],
        [F.avg(col("tip") / col("total_bill")).alias("tip_pct")],
    )
    .filter(col("day") != lit("Dinner"))
    .aggregate([col("sex"), col("smoker")], [F.avg(col("tip_pct")).alias("avg_pct")])
)

ray_results = ray_ctx.plan(df.execution_plan())
df_ctx.create_dataframe([[ray_results]]).show()
