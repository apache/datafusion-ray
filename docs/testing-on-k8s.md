# Testing in Kubernetes

This guide explains how to test DataFusion Ray on Kubernetes during development. It assumes you have an existing Kubernetes cluster.

## 1. Deploy the KubeRay Operator

To manage Ray clusters, you need to deploy the KubeRay operator using Helm. This step is required once per Kubernetes cluster.

```shell
helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm repo update

# Install the Custom Resource Definitions (CRDs) and KubeRay operator
helm install kuberay-operator kuberay/kuberay-operator

# Verify that the operator is running in the `default` namespace.
kubectl get pods

# Example output:
# NAME                                READY   STATUS    RESTARTS   AGE
# kuberay-operator-7fbdbf8c89-pt8bk   1/1     Running   0          27s
```

You can customize the operator's settings (e.g., resource limits and requests). For basic testing, the default configuration should suffice.
For more details and customization options, refer to the [KubeRay Helm Chart documentation](https://github.com/ray-project/kuberay-helm/tree/main/helm-chart/kuberay-operator).

## 2. Build a Custom Docker Image
You need to build a custom Docker image containing your local development copy of DataFusion Ray rather than using the default PyPi release.

Run the following command to build your Docker image:

```shell
docker build -t [YOUR_IMAGE_NAME]:[YOUR_TAG] -f k8s/Dockerfile .
```
After building the image, push it to a container registry accessible by your Kubernetes cluster.

## 3. Deploy a RayCluster
Next, deploy a RayCluster using the custom image.

```shell
helm repo update
helm install datafusion-ray kuberay/ray-cluster \
    --set 'image.repository=andygrove/datafusion-ray' \
    --set 'image.tag=latest' \
    --set 'imagePullPolicy=Always'
```
Make sure you replace *[YOUR_REPOSITORY]* and *[YOUR_TAG]* with your actual container registry and image tag values.

You can further customize RayCluster settings (such as resource allocations, autoscaling, and more).
For full configuration options, refer to the [RayCluster Helm Chart documentation](https://github.com/ray-project/kuberay-helm/tree/main/helm-chart/ray-cluster).

## 4. Port Forwarding

To access Ray's dashboard, set up port forwarding between your local machine and the Ray cluster's head node:

```shell
kubectl port-forward service/raycluster-kuberay-head-svc 8265:8265
```

This makes Ray’s dashboard and API available at `http://127.0.0.1:8265`.


## 5. Run an Example
From the examples directory in your project, you can run a sample job using the following commands:

```
export RAY_ADDRESS="http://127.0.0.1:8265"
ray job submit --working-dir ./examples/ -- python3 tips.py
```

### Expected output:
