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

# Benchmarking DataFusion Ray on Kubernetes

This is a rough guide to deploying and benchmarking DataFusion Ray on Kubernetes as part of the development process.

## Building Wheels

Follow the instructions in the [contributor guide] to set up a development environment and then build the project 
using the following command.

[contributor guide]: ../docs/contributing.md

```shell
maturin build --strip
```

## Create a Ray Cluster

Follow the instructions in the [Ray on Kubernetes] documentation to install the KubeRay operator.

[Ray on Kubernetes]: https://docs.ray.io/en/latest/cluster/kubernetes/index.html

Create a `datafusion-ray.yaml` file using the following template. It is important that the Ray Docker image uses the 
same Python version that was used to build the wheels. This example yaml assumes that the TPC-H data files are 
available locally on each node in the cluster at the path `/mnt/bigdata`. If the data is stored on object storage then 
the `volume` and `volumeMount` sections can be removed.

```yaml
apiVersion: ray.io/v1alpha1
kind: RayCluster
metadata:
  name: datafusion-ray-cluster
spec:
  headGroupSpec:
    rayStartParams:
      num-cpus: "0"
    template:
      spec:
        containers:
          - name: ray-head
            image: rayproject/ray:2.42.1-py310-cpu
            imagePullPolicy: Always
            resources:
              limits:
                cpu: 2
                memory: 8Gi
              requests:
                cpu: 2
                memory: 8Gi
            volumeMounts:
              - mountPath: /mnt/bigdata  # Mount path inside the container
                name: ray-storage
        volumes:
          - name: ray-storage
            persistentVolumeClaim:
              claimName: ray-pvc
  workerGroupSpecs:
    - replicas: 2
      groupName: "datafusion-ray"
      rayStartParams:
        num-cpus: "4"
      template:
        spec:
          containers:
            - name: ray-worker
              image: rayproject/ray:2.42.1-py310-cpu
              imagePullPolicy: Always
              resources:
                limits:
                  cpu: 5
                  memory: 64Gi
                requests:
                  cpu: 5
                  memory: 64Gi
              volumeMounts:
                - mountPath: /mnt/bigdata
                  name: ray-storage
          volumes:
            - name: ray-storage
              persistentVolumeClaim:
                claimName: ray-pvc
```

Run the following command to create the cluster:

```shell
kubectl apply -f datafusion-ray.yaml
```

Once the cluster is running, set up port forwarding on port 8265 on the head node and then run the following 
command to run the benchmarks.

```shell
ray job submit --address='http://localhost:8265' \
  --runtime-env-json='{"pip":["datafusion", "tabulate", "boto3", "duckdb"], "py_modules":["/home/andy/git/apache/datafusion-ray/target/wheels/datafusion_ray-0.1.0-cp38-abi3-manylinux_2_35_x86_64.whl"], "working_dir":"./", "env_vars":{"RAY_DEDUP_LOGS":"O", "RAY_COLOR_PREFIX":"1"}}' -- \
  python tpcbench.py \
  --data /mnt/bigdata/tpch/sf100 \
  --concurrency 8 \
  --partitions-per-worker 4 \
  --worker-pool-min 30 \
  --listing-tables
```
