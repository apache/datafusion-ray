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

Import the image into k3s

```shell
docker save dfray | k3s ctr images import -
```

## Run the example

From the `examples` directory:

```shell
ray job submit --working-dir `pwd` --runtime-env ../k8s/runtime-env.yaml -- python3 tips.py
```
