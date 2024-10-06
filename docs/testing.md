# Distributed Testing

Install Ray on at least one nodes. 

https://docs.ray.io/en/latest/ray-overview/installation.html

```shell
sudo apt install -y python3-pip python3.12-venv
python3 -m venv venv
source venv/bin/activate
pip3 install -U "ray[default]"
```

## Start Ray Head Node

```shell
 ray start --head --dashboard-host 0.0.0.0 --include-dashboard true
```

## Start Ray Worker Nodes(s) (Optional, and MacOS doesn't support multi-node cluster)

```shell
ray start --address=127.0.0.1:6379
```

## Install DataFusion Ray (on each node)

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

## Submit Job

1. If starting the cluster manually, simply connect to the existing cluster instead of reinitializing it.
```
# Start a local cluster
# ray.init(resources={"worker": 1})

# Connect to a cluster
ray.init()
```

2. Submit the job to Ray Cluster
```shell
RAY_ADDRESS='http://10.0.0.23:8265'  ray job submit --working-dir examples -- python3 tips.py
```
