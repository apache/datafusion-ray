# Testing in Kubernetes

This guide explains how to test on k8s during development of DataFusion Ray.

## Setting up a Ray cluster

Follow instructions from https://docs.ray.io/en/latest/cluster/kubernetes/getting-started/raycluster-quick-start.html#kuberay-raycluster-quickstart

## Build Custom Docker Image

We need to use a custom Docker image that contains a packaged version of our local development copy of DataFusion Ray 
instead of pulling the latest release from PyPi.

Run the following command to build the Docker image.

```shell
docker build -t dfray -f k8s/Dockerfile .
```

Either push the image to a repository that k3s can pull from (recommended), or copy the image directly into the nodes:

```shell
docker save -o dfray.tar dfray
scp dfray.tar username@node1:
scp dfray.tar username@node2:
```

ssh into each node and run:

```shell
sudo k3s ctr images import dfray.tar
```

## Run the example

From the `examples` directory:

```shell
ray job submit --runtime-env-json='{"container": {"image": "dfray"}}' -- python3 tips.py
```

This fails with:

```text
 'The 'container' field currently cannot be used together with other fields of runtime_env. Specified fields: dict_keys(['container', 'env_vars'])'
```

Possible fix: https://github.com/ray-project/ray/pull/42121