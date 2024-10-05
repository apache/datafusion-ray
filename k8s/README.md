# Testing in Kubernetes

This guide explains how to test on k8s during development of DataFusion Ray.

## Setting up a Kubernetes cluster

Install k3s.

## Setting up a Ray cluster

Follow instructions from https://docs.ray.io/en/latest/cluster/kubernetes/getting-started/raycluster-quick-start.html#kuberay-raycluster-quickstart

Manually update the ray image version from `2.9.0` to `2.37.0.cabc24-py312`

Install podman on the nodes. We need this to use Ray's experimental custom Docker container support.

```shell
sudo apt update
sudo apt install -y podman
```

Set up port forwarding.

```shell
kubectl port-forward service/raycluster-kuberay-head-svc 8265:8265
```

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

This currently fails with:

```shell

-------------------------------------------------------
Job 'raysubmit_nsmAj16vXAtMc2Dm' submitted successfully
-------------------------------------------------------

Next steps
  Query the logs of the job:
    ray job logs raysubmit_nsmAj16vXAtMc2Dm
  Query the status of the job:
    ray job status raysubmit_nsmAj16vXAtMc2Dm
  Request the job to be stopped:
    ray job stop raysubmit_nsmAj16vXAtMc2Dm

Tailing logs until the job exits (disable with --no-wait):

---------------------------------------
Job 'raysubmit_nsmAj16vXAtMc2Dm' failed
---------------------------------------

Status message: runtime_env setup failed: Failed to set up runtime environment.
Could not create the actor because its associated runtime env failed to be created.
```

The root cause appears to be:

```shell
FileNotFoundError: [Errno 2] No such file or directory: 'podman'
```