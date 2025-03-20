## Benchmarking on kubernetes

This directory contains a utility `bench_toolbox.py` to facilitate benchmarking spark and datafusion-ray on k8s clusters.

The paved path is to execute the steps on a fresh 24.04 ubuntu ami, but the tool should also work on for established k8s setups.

If that is the case you'll want to skip the install of `https://k3s.io/` from the `k3s` subcommand and proceed. The `machine_prep.sh` script should provide clues about the environment requirements you'll need to satisfy to operate th tool.

### Benchmarking on a fresh ubuntu 24.04 LTS ami from amazon

- provision the machine and ssh in and download the repo

```bash
git checkout https://github.com/robtandy/datafusion-ray
cd datafusion-ray
git checkout k8s_benchmarking
```

- then run the machine prep script

````bash
cd datafusion-ray/k8s
```bash
./machine_prep.sh
````

Next, you'll want to choose where you'll keep your TPCH data.

```bash
sudo mkdir /data
sudo chmod -R 777 /data
```

At this point, you'll have the configuration needed to operate the `bench_toolbox.py` script. So, if you first need kubernetes installed, run

```bash
./bench_toolbox.py -v k3s --data-path /data
```

This will:

- create a single machine cluster using k3s
- create the PVC for /data
- install kuberay operater
- install spark operator


