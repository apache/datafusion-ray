#!/usr/bin/env python3
import click

import cmds
import os
import pandas as pd
import glob
import json
import re
import pricing
import time
import datafusion
import ec2_metadata
import subprocess
from cmds import Runner

runner: Runner | None = None


@click.group()
@click.option("--dry-run", is_flag=True)
@click.option("-v", "--verbose", is_flag=True)
def cli(dry_run: bool, verbose: bool):
    global runner
    runner = Runner(dry_run, verbose)


@cli.command(help="run spark and df ray benchmarks")
@click.option(
    "--executor-cpus",
    type=int,
    help="how much cpu to allocate to the executor[ray worker] nodes.",
    required=True,
)
@click.option(
    "--executor-mem",
    type=int,
    help="how much memory (GiB) to allocate to the executor[ray worker] nodes.",
    required=True,
)
@click.option(
    "--executor-overhead-mem",
    type=int,
    help="how much memory (GiB) to allocate to the executor overhead.  Not used on ray.  Will be subtracted from executor_mem",
    required=True,
)
@click.option(
    "--executor-num",
    type=int,
    help="how many executors[ray workers] to start",
    required=True,
)
@click.option(
    "--driver-mem",
    type=int,
    help="how much memory (GiB) to allocate to the driver[head] node.",
    required=True,
)
@click.option(
    "--driver-cpus",
    type=int,
    help="how much cpu to allocate to the driver[ray head] node.",
    required=True,
)
@click.option(
    "--scale-factor",
    type=click.Choice(["1", "10", "100", "1000"]),
    help="TPCH scale factor",
    required=True,
)
@click.option(
    "--data-path",
    type=str,
    help="path(url) to the directory that holds generated TPCH data.  Should be >= 300GB",
    required=True,
)
@click.option(
    "--output-path",
    type=str,
    help="path to the local directory exposed via PVC",
    required=True,
)

@click.option(
    "--concurrency",
    type=int,
    help="DFRay only.  The number of target partitions to use in planning",
    required=True,
)
@click.option(
    "--partitions-per-processor",
    type=int,
    help="how many partitions (out of [concurrency] value to host in each DFRayProcessor",
    required=True,
)
@click.option(
    "--processor-pool-min",
    type=int,
    help="minimum number of DFRayProcessrs to allocate in a pool for use by queries",
    required=True,
)
@click.option(
    "--df-ray-version", type=str, help="version number of DFRay to use", required=True
)
@click.option(
    "--test-pypi",
    is_flag=True,
    help="use the test.pypi upload of DFRay",
)
@click.option(
    "--arm",
    is_flag=True,
    help="deploy an arm image for ray cluster image",
)
@click.argument(
    "system",
    type=click.Choice(["spark", "df_ray"]),
)
def bench(system, **kwargs):
    assert runner is not None
    match system:
        case "spark":
            runner.run_commands(cmds.cmds["bench_spark"], kwargs)
        case "df_ray":
            runner.run_commands(cmds.cmds["bench_df_ray"], kwargs)
        case _:
            print(f"unknown system {system}")
            exit(1)


@click.option(
    "--data-path",
    type=str,
    help="path/url to the directory that holds generated TPCH data.  Should be >= 300GB",
    required=True,
)
@click.option(
    "--output-path",
    type=str,
    help="path where outputfiles are written",
    required=True,
)
@click.option(
    "--data-device",
    type=str,
    help="path to the device in /dev/ that holds the data-path.  It will be benchmarked with hdparm for throughput.",
    required=True,
)
@click.option(
    "--scale-factor",
    type=click.Choice(["1", "10", "100", "1000"]),
    help="TPCH scale factor",
    required=True,
)
@cli.command(help="assemble the results into a single json")
def results(data_path, data_device, scale_factor, output_path):
    df_result = json.loads(
        open(
            newest_file(glob.glob(os.path.join(output_path, "datafusion-ray*json")))
        ).read()
    )
    spark_result = json.loads(
        open(newest_file(glob.glob(os.path.join(output_path, "spark-tpch*json")))).read()
    )
    print(df_result)
    print(spark_result)
    total_results = {"spark": spark_result, "df-ray": df_result}

    spark = [spark_result["queries"][f"{i}"] for i in range(1, 23)]
    df_ray = [df_result["queries"][f"{i}"] for i in range(1, 23)]

    # add a final row with the totals
    spark += [sum(spark)]
    df_ray += [sum(df_ray)]

    # add another final row with costs

    # df for "dataframe" here, not "datafusion".  Just using pandas for easy output
    df = pd.DataFrame({"spark": spark, "df_ray": df_ray})
    df["change"] = df["df_ray"] / df["spark"]

    df["change_text"] = df["change"].apply(
        lambda change: (
            f"+{(1 / change):.2f}x faster" if change < 1.0 else f" {change:.2f}x slower"
        )
    )
    df["tpch_query"] = [f"{i}" for i in range(1, 23)] + ["total"]
    df["sort_index"] = list(range(1, 24))

    ts = time.time()
    df.to_parquet(f"datafusion-ray-spark-comparison-{ts}.parquet")
    ctx = datafusion.SessionContext()
    ctx.register_parquet("results", f"datafusion-ray-spark-comparison-{ts}.parquet")

    cpu = subprocess.run(
        "lscpu | grep 'Model name' |awk '{print $3}'",
        shell=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    quantity = subprocess.run(
        "lscpu | grep '^CPU(s):' |awk '{print $2}'",
        shell=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    memory = subprocess.run(
        "lsmem | grep 'Total online' |awk '{print $4}'",
        shell=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    hdresults = subprocess.run(
        f"sudo hdparm -t {data_device}|grep 'MB/sec'",
        shell=True,
        capture_output=True,
        text=True,
    ).stdout.strip()

    hdresult = re.search(r"([\d\.]+) MB/sec", hdresults, re.MULTILINE).group(1)

    machine = ec2_metadata.ec2_metadata.instance_type

    # if you get reserved it includes any discounts you may have associated
    # with your ec2 credentials.  So a public price is appropriate for sharing
    hourly_cost = pricing.get_on_demand_price("us-east-1", machine)

    spark_cost = spark[-1] / 3600 * hourly_cost
    df_ray_cost = df_ray[-1] / 3600 * hourly_cost

    cost_delta = df_ray_cost / spark_cost
    cost_delta_text = (
        f"+{(1 / cost_delta):.2f}x cheaper"
        if cost_delta < 1.0
        else f"{cost_delta:.2f}x more expensive"
    )
    speed_delta_text = df["change_text"].iloc[-1]

    df_ray_cost = f"${df_ray_cost:.4f}"
    df_ray_duration = f"{df_ray[-1]:.2f}s"
    spark_cost = f"${spark_cost:.4f}"
    spark_duration = f"{spark[-1]:.2f}s"

    print("=" * 97)
    # the formatting code is terrible here, but it works for now
    header = [
        "Spark and DataFusionRay TPCH 100 Benchmarks",
        f"{'Machine:':<30}{machine}",
        f"{'Machine On Demand Cost:':<30}{hourly_cost} $/hr",
        f"{'CPU(s):':<30}{cpu} {quantity}x",
        f"{'MEM:':<30}{memory}",
        f"{'HD Throughput:':<30}{hdresult} MB/s (from hdparm)",
        f"{'Data Location:':<30}{data_path}/sf{scale_factor}",
        "",
        f"{'df-ray duration:':<30}{df_ray_duration:>10} {speed_delta_text}",
        f"{'df-ray cost:':<30}{df_ray_cost:>10} {cost_delta_text}",
        "",
        f"{'spark duration:':<30}{spark_duration:>10}",
        f"{'spark cost:':<30}{spark_cost:>10}",
        "",
        "DataFusionRay Settings:",
        f"{'concurrency:':<30}{df_result['settings']['concurrency']:>10}",
        f"{'batch_size :':<30}{df_result['settings']['batch_size']:>10}",
        f"{'partitions_per_processor:':<30}{df_result['settings']['partitions_per_processor']:>10}",
        f"{'Ray Workers:':<30}{spark_result['spark_conf']['spark.executor.instances']:>10}",
        f"{'Ray Worker Mem (GB):':<30}{int(spark_result['spark_conf']['spark.executor.memory'][:-1]) + int(spark_result['spark_conf']['spark.executor.memoryOverhead'][:-1]):>10}",
        f"{'Ray Worker CPU:':<30}{spark_result['spark_conf']['spark.executor.cores']:>10}",
        f"{'Ray Head Mem (GB):':<30}{int(spark_result['spark_conf']['spark.driver.memory'][:-1]):>10}",
        f"{'Ray Head CPU:':<30}{spark_result['spark_conf']['spark.driver.cores']:>10}",
        "",
        "Spark Settings:",
        f"{'Executors:':<30}{spark_result['spark_conf']['spark.executor.instances']:>10}",
        f"{'Executor Mem (GB):':<30}{int(spark_result['spark_conf']['spark.executor.memory'][:-1]):>10}",
        f"{'Executor Overhead Mem (GB):':<30}{int(spark_result['spark_conf']['spark.executor.memoryOverhead'][:-1]):>10}",
        f"{'Executor CPU:':<30}{spark_result['spark_conf']['spark.executor.cores']:>10}",
        f"{'Driver Mem(GB):':<30}{int(spark_result['spark_conf']['spark.driver.memory'][:-1]):>10}",
        f"{'Driver CPU:':<30}{spark_result['spark_conf']['spark.driver.cores']:>10}",
    ]
    for h in header:
        print(h)

    print("=" * 97)
    ctx.sql(
        'select tpch_query, spark, df_ray, change as "change(=df_ray/spark)", change_text from results order by sort_index asc'
    ).show(num=100)

    out_path = f"datafusion-ray-spark-comparison-{ts}.json"
    open(out_path, "w").write(json.dumps(total_results))


@cli.command(help="Install k3s and configure it")
@click.option(
    "--data-path",
    type=str,
    help="path to the directory that holds generated TPCH data.  Should be >= 300GB",
    required=True,
)
@click.option(
    "--k3s-url",
    type=str,
    help="url to head node of the cluster to join",
)
@click.option(
    "--k3s-token",
    type=str,
    help="k3s token to authorize when joining the cluster",
)
def k3s(**kwargs):
    assert runner is not None
    if kwargs["k3s_url"]:
        kwargs["k3s_url"] = f"K3S_URL={kwargs['k3s_url']}"
    if kwargs["k3s_token"]:
        kwargs["k3s_token"] = f"K3S_TOKEN={kwargs['k3s_token']}"
    runner.run_commands(cmds.cmds["k3s_setup"], kwargs)


@cli.command(help="Generate TPCH data")
@click.option(
    "--data-path",
    type=str,
    help="path to the directory that will hold the generated TPCH data.  Should be >= 300GB",
    required=True,
)
@click.option(
    "--scale-factor",
    type=click.Choice(["1", "10", "100", "1000"]),
    help="TPCH scale factor",
    required=True,
)
@click.option(
    "--partitions",
    type=int,
    help="TPCH number of partitions for each table",
    required=True,
)
@click.option(
    "--pool-size",
    type=int,
    default=1,
    help="number of concurrent processors to use.  Watch out!  too high and machine will lock up from too much memory use",
)
def generate(**kwargs):
    assert runner is not None
    runner.run_commands(cmds.cmds["generate"], kwargs)


@cli.command(help="just testing of toolbox shell commands that are harmless")
def echo():
    assert runner is not None
    runner.run_commands(cmds.cmds["echo"])


@cli.command()
def help():
    """Print the overall help message."""
    click.echo(cli.get_help(click.Context(cli)))


def newest_file(files: list[str]):
    return max(files, key=os.path.getctime)


if __name__ == "__main__":
    cli()
