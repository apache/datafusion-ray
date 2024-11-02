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

# TPC-H

## Running Benchmarks

### Standalone Ray Cluster

Data and queries must be available on all nodes of the Ray cluster.

```shell
 RAY_ADDRESS='http://ray-cluster-ip-address:8265'  ray job submit --working-dir `pwd` -- python3 tpcbench.py --benchmark tpch --data /path/to/data --queries /path/to/tpch/queries
```

### Kubernetes

Create a Docker image containing the TPC-H queries and push to a Docker registry that is accessible from the k8s cluster.

```shell
docker build -t YOURREPO/datafusion-ray-tpch .
```

If the data files are local to the k8s nodes, then create a persistent volume and persistent volume claim.

Create a `pv.yaml` with the following content and run `kubectl apply -f pv.yaml`.

```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: ray-pv
spec:
  storageClassName: manual
  capacity:
    storage: 10Gi
  accessModes:
    - ReadWriteOnce
  hostPath:
    path: "/mnt/bigdata"  # Adjust the path as needed
```

Create a `pvc.yaml` with the following content and run `kubectl apply -f pvc.yaml`.

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: ray-pvc
spec:
  storageClassName: manual  # Should match the PV's storageClassName if static
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 10Gi
```

Create the Ray cluster using the custom image.

Create a `ray-cluster.yaml` with the following content and run `kubectl apply -f ray-cluster.yaml`.

```yaml
apiVersion: ray.io/v1alpha1
kind: RayCluster
metadata:
  name: datafusion-ray-cluster
spec:
  headGroupSpec:
    rayStartParams:
      num-cpus: "1"
    template:
      spec:
        containers:
          - name: ray-head
            image: YOURREPO/datafusion-ray-tpch:latest
            volumeMounts:
              - mountPath: /mnt/bigdata  # Mount path inside the container
                name: ray-storage
        volumes:
          - name: ray-storage
            persistentVolumeClaim:
              claimName: ray-pvc  # Reference the PVC name here
  workerGroupSpecs:
    - replicas: 2
      groupName: "datafusion-ray"
      rayStartParams:
        num-cpus: "4"
      template:
        spec:
          containers:
            - name: ray-worker
              image: YOURREPO/datafusion-ray-tpch:latest
              volumeMounts:
                - mountPath: /mnt/bigdata
                  name: ray-storage
          volumes:
            - name: ray-storage
              persistentVolumeClaim:
                claimName: ray-pvc
```

Run the benchmarks

```shell
ray job submit --working-dir `pwd` -- python3 tpcbench.py --benchmark tpch --queries /home/ray/datafusion-benchmarks/tpch/queries/ --data /mnt/bigdata/tpch/sf100
```