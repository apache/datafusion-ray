# Distributed Testing

Install Ray on at least two nodes. 

https://docs.ray.io/en/latest/ray-overview/installation.html

```shell
sudo apt install -y python3-pip python3.12-venv
python3 -m venv venv
source venv/bin/activate
pip3 install -U "ray[default]"
```

## Start Ray Head Node

```shell
ray start --head --node-ip-address=10.0.0.23 --port=6379 --dashboard-host=0.0.0.0
```

## Start Ray Worker Nodes(s)

```shell
ray start --address=10.0.0.23:6379 --redis-password='5241590000000000'
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

```shell
cd examples
RAY_ADDRESS='http://10.0.0.23:8265'  ray job submit --working-dir `pwd` -- python3 tips.py
```
