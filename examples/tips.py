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

import argparse
import datafusion
import ray

from datafusion_ray import RayContext


def go(data_dir: str):
    ctx = RayContext()
    ctx.set("datafusion.execution.parquet.pushdown_filters", "true")

    # we could set this value to however many CPUs we plan to give each
    # ray task
    # ctx.set("datafusion.optimizer.enable_round_robin_repartition", "false")
    ctx.set("datafusion.execution.target_partitions", "1")

    ctx.register_parquet("tips", f"{data_dir}/tips*.parquet")

    df = ctx.sql(
        "select sex, smoker, avg(tip/total_bill) as tip_pct from tips group by sex, smoker"
    )
    df.show()

    print("no ray result:")

    # compare to non ray version
    ctx = datafusion.SessionContext()
    ctx.register_parquet("tips", f"{data_dir}/tips*.parquet")
    ctx.sql(
        "select sex, smoker, avg(tip/total_bill) as tip_pct from tips group by sex, smoker"
    ).show()


if __name__ == "__main__":
    ray.init(namespace="tips")
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", required=True, help="path to tips*.parquet files")
    args = parser.parse_args()

    go(args.data_dir)

    import time

    time.sleep(3)
