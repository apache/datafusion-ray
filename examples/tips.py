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

from datafusion_ray import RayContext

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

# Connect to a cluster
ray.init()

ctx = RayContext()
ctx.set("datafusion.execution.parquet.pushdown_filters", "true")

# we could set this value to however many CPUs we plan to give each
# ray task
ctx.set("datafusion.optimizer.enable_round_robin_repartition", "false")
ctx.set("datafusion.execution.target_partitions", "4")

ctx.register_parquet("tips", f"{SCRIPT_DIR}/tips*.parquet")

df = ctx.sql(
    "select sex, smoker, avg(tip/total_bill) as tip_pct from tips group by sex, smoker"
)

print(df.execution_plan().display_indent())

df.show()
