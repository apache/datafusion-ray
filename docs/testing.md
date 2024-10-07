# Testing

* [Single Node Testing](#Single-Node-Testing)
* [Distributed Testing](#Distributed-Testing)
* [Ray Installation Docs](https://docs.ray.io/en/latest/ray-overview/installation.html)

## Single Node Testing

Install Ray on one (head) node. 

```shell
sudo apt install -y python3-pip python3.12-venv
python3 -m venv venv
source venv/bin/activate
pip3 install -U "ray[default]"
```

### Start Ray Head Node

```shell
 ray start --head --dashboard-host 0.0.0.0 --include-dashboard true
```

### Start Ray Worker Nodes(s) (Optional)

This is optional, if you go add Ray worker noeds, it becomes distributed.

Also [Ray doesn't support MacOS multi-node cluster](https://docs.ray.io/en/latest/cluster/getting-started.html#where-can-i-deploy-ray-clusters)

```shell
ray start --address=127.0.0.1:6379
```

### Install DataFusion Ray (on head node)

Clone the repo with the version that you want to test. Run `maturin build --release` in the virtual env.

```shell
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
. "$HOME/.cargo/env"
```

```shell
pip3 install maturin
```

```shell
git clone https://github.com/apache/datafusion-ray.git
cd datafusion-ray
maturin develop --release
```

### Submit Job

1. If started the cluster manually, simply connect to the existing cluster instead of reinitializing it.
```python
# Start a local cluster
# ray.init(resources={"worker": 1})

# Connect to a cluster
ray.init()
```

2. Submit the job to Ray Cluster
```shell
RAY_ADDRESS='http://127.0.0.1:8265' ray job submit --working-dir examples -- python3 tips.py
```

## Distributed Testing

Install Ray on at least two nodes. 

```shell
sudo apt install -y python3-pip python3.12-venv
python3 -m venv venv
source venv/bin/activate
pip3 install -U "ray[default]"
```

### Start Ray Head Node

```shell
ray start --head --dashboard-host 0.0.0.0 --include-dashboard true
```

### Start Ray Worker Nodes(s)

Replace `NODE_IP_ADDRESS` with the address accessible in your distributed setup, which will be displayed after the previous step.

```shell
ray start --address={NODE_IP_ADDRESS}:6379
```

### Install DataFusion Ray (on each node)

Clone the repo with the version that you want to test. Run `maturin build --release` in the virtual env.

```shell
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
. "$HOME/.cargo/env"
```

```shell
pip3 install maturin
```

```shell
git clone https://github.com/apache/datafusion-ray.git
cd datafusion-ray
maturin develop --release
```

### Submit Job

1. If starting the cluster manually, simply connect to the existing cluster instead of reinitializing it.

```python
# Start a local cluster
# ray.init(resources={"worker": 1})

# Connect to a cluster
ray.init()
```

2. Submit the job to Ray Cluster

```shell
RAY_ADDRESS='http://{NODE_IP_ADDRESS}:8265' ray job submit --working-dir examples -- python3 tips.py
```
