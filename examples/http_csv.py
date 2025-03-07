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

# this is a port of the example at
# https://github.com/apache/datafusion/blob/45.0.0/datafusion-examples/examples/query-http-csv.rs

import ray

from datafusion_ray import DFRayContext, df_ray_runtime_env


def main():
    ctx = DFRayContext()
    ctx.register_csv(
        "aggregate_test_100",
        "https://github.com/apache/arrow-testing/raw/master/data/csv/aggregate_test_100.csv",
    )

    df = ctx.sql("SELECT c1,c2,c3 FROM aggregate_test_100 LIMIT 5")

    df.show()


if __name__ == __name__:
    ray.init(namespace="http_csv", runtime_env=df_ray_runtime_env)
    main()
