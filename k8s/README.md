## Benchmarking on kubernetes

This directory contains a utility `bench_toolbox.py` to facilitate benchmarking spark and datafusion-ray on k8s clusters.

The paved path is to execute the steps on a fresh 24.04 ubuntu ami, but the tool should also work on for established k8s setups.

If that is the case you'll want to skip the install of `https://k3s.io/` from the `k3s` subcommand and proceed. The `machine_prep.sh` script should provide clues about the environment requirements you'll need to satisfy to operate th tool.

### Current status

Help wanted! This code is rough in that it has too many steps and doesn't handle enough variety in machine/architecture to run unattended. PRs welcome to improve any and all of what is here.

#### Known issues and quirks

- These instructions are likely incomplete
- benchmark results for df_ray and spark do not ensure the same settings are used, they require the operator use the same versions
- the results subcommand just looks for the newest DataFusion for Ray and Spark results, and assumes those are the correct ones
- the machine_prep script does not handle errors

### Sample results

You can find results conducted with different versions of DataFusion for Ray in `docs/benchmarks/[dfray version]`

### Benchmarking on a fresh ubuntu 24.04 LTS ami from amazon

- provision the machine and ssh in and download the repo

```bash
git checkout https://github.com/robtandy/datafusion-ray
cd datafusion-ray
git checkout k8s_benchmarking
```

then run the machine prep script

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

Next, lets generate TPCH data in the /data directory, scale factor 1 to start

```bash
./bench_toolbox.py -v generate --data-path /data --scale-factor 1 --partitions 2 --pool-size 2
```

Now we can run a benchmark with the generated data with DataFusion for Ray

```bash
./bench_toolbox.py -v bench --executor-cpus 2 --executor-mem 10 --executor-num 2 --executor-overhead-mem 4 --driver-mem 4 --driver-cpus 2 --data-path /data --concurrency 8 --partitions-per-processor 4 --processor-pool-min 50 --df-ray-version 0.1.0rc1 --test-pypi --arm --scale-factor 1 --output-path /data df_ray
```

followed by spark. Make sure you use the same settings

```bash
./bench_toolbox.py -v bench --executor-cpus 2 --executor-mem 10 --executor-num 2 --executor-overhead-mem 4 --driver-mem 4 --driver-cpus 2 --data-path /data --concurrency 8 --partitions-per-processor 4 --processor-pool-min 50 --df-ray-version 0.1.0rc1 --test-pypi --arm --scale-factor 1 --output-path /data spark
```

Lastly, compile the results:

```bash
./bench_toolbox.py -v results --data-device /dev/nvme1n1 --data-path /data --scale-factor 1 --output-path /data
```

You should get a table of results similar to what you'd find in `docs/benchmarks`
