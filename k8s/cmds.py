from collections import namedtuple
import subprocess
import jinja2
import click
import os
import signal
import textwrap

Shell = namedtuple("Shell", ["cmd", "desc"])
BackgroundShell = namedtuple("BackgroundShell", ["cmd", "desc"])
Template = namedtuple("Template", ["path", "desc"])
ChangeDir = namedtuple("ChangeDir", ["path", "desc"])
Venv = namedtuple("Venv", ["cmd", "path", "desc"])

MY_DIR = os.path.dirname(os.path.abspath(__file__))

# TODO: assert that commands we require like python3, jq, curl, are all present

cmds = {
    "echo": [
        Shell("echo hello 1", "echoing first"),
        Shell("echo hello 2", "echoing second"),
        Shell("bad_command_garbage", "Something that will fail"),
        Shell("echo hello 3", "echoing third which we wont see"),
    ],
    "k3s_setup": [
        Shell(
            """sudo curl -sfL https://get.k3s.io | {{ k3s_url if k3s_url else "" }} {{ k3s_token if k3s_token else ""}} sh -s -""",
            "Installing K3s",
        ),
        Shell(
            "sudo chmod a+r /etc/rancher/k3s/k3s.yaml",
            "Allow read access to chmod a+r /etc/rancher/k3s/k3s.yaml",
        ),
        Shell(
            "kubectl get all",
            "Check for access to cluster",
        ),
        Shell(
            "curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash",
            "Installing Helm",
        ),
        Shell(
            "helm --kubeconfig /etc/rancher/k3s/k3s.yaml repo add kuberay https://ray-project.github.io/kuberay-helm/",
            "Adding kube ray helm repo",
        ),
        Shell(
            "helm --kubeconfig /etc/rancher/k3s/k3s.yaml repo add spark-operator https://kubeflow.github.io/spark-operator",
            "Adding spark operator helm repo",
        ),
        Shell("helm repo update", "Updating helm repos"),
        Shell(
            "helm --kubeconfig /etc/rancher/k3s/k3s.yaml install kuberay-operator kuberay/kuberay-operator --version 1.3.0 --wait",
            "Installing kuberay-operator",
        ),
        Shell(
            """helm --kubeconfig /etc/rancher/k3s/k3s.yaml install --set-json='controller.env=[{"name":"SPARK_SUBMIT_OPTS","value":"-Divy.cache.dir=/tmp/ivy2/cache -Divy.home=/tmp/ivy2"}]' spark-operator spark-operator/spark-operator""",
            "Installing spark-operator",
        ),
        Template("pvcs.yaml.template", "rewrite pvcs.yaml.template"),
        Shell("kubectl apply -f pvcs.yaml", "Apply pvcs"),
    ],
    "generate": [
        Shell(
            "mkdir -p /data/sf{{scale_factor}}",
            "make directory /data/sf{{scale_factor}}",
        ),
        Shell(
            "python {{ MY_DIR }}/../tpch/make_data.py {{scale_factor}} {{partitions}} {{data_path}}/sf{{scale_factor}} {{pool_size}}",
            "generate data",
        ),
    ],
    "bench_spark": [
        Template("spark_job.yaml.template", "rewrite spark_job.yaml.template"),
        Shell(
            "cp {{ MY_DIR }}/spark_tpcbench.py {{ output_path }}",
            "copy spark_tpcbench.py to data_path dir",
        ),
        Shell(
            "cp -a {{ MY_DIR }}/../tpch/queries {{ output_path }}",
            "copy tpch queries to data_path dir",
        ),
        Shell(
            """
            {% if data_path.startswith("s3") %}
            wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
            wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
            aws s3 cp aws-java-sdk-bundle-1.12.262.jar {{ data_path.replace('s3a','s3') }}/aws-java-sdk-bundle-1.12.262.jar && \
            aws s3 cp hadoop-aws-3.3.4.jar {{ data_path.replace('s3a','s3') }}/hadoop-aws-3.3.4.jar
            {% else %}
            wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
            wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
            mv aws-java-sdk-bundle-1.12.262.jar {{ data_path }}/aws-java-sdk-bundle-1.12.262.jar && \
            mv hadoop-aws-3.3.4.jar {{ data_path }}/hadoop-aws-3.3.4.jar

            {% endif %}
            """,
            "getting additional spark jars",
        ),
        Shell(
            "kubectl apply -f spark_job.yaml",
            "Submit spark job",
        ),
        Shell(
            """
            while true; do
                sleep 10 
                STATE=$(kubectl get sparkapp/spark-tpch-bench -o json |jq -r '.status.applicationState.state')
                echo  "Checking on job status...got $STATE looking for COMPLETED"
                if [[ $STATE != "RUNNING" ]]; then
                    break
                fi
            done
            """,
            "checking on job status",
        ),
        Shell(
            "kubectl delete -f spark_job.yaml",
            "tear down job",
        ),
    ],
    "bench_df_ray": [
        Template("ray_cluster.yaml.template", "rewrite ray_cluster.yaml.template"),
        Shell(
            "kubectl apply -f ray_cluster.yaml",
            "deploying ray cluster",
        ),
        Shell(
            "kubectl wait raycluster/datafusion-ray-cluster --for='jsonpath={.status.state}'=ready --timeout=300s",
            "wait for ray cluster to be ready",
        ),
        Template("requirements.txt.template", "rewrite requirements.txt.template"),
        Template("ray_job.sh.template", "rewrite ray_job.sh.template"),
        BackgroundShell(
            "kubectl port-forward svc/datafusion-ray-cluster-head-svc 8265:8265",
            "port forwarding from cluster",
        ),
        Shell(
            "cp {{ MY_DIR }}/../tpch/tpcbench.py .",
            "copy tpcbench.py to .",
        ),
        Shell(
            "cp -a {{ MY_DIR }}/../tpch/queries .",
            "copy tpch queries to .",
        ),
        Shell(
            ". ./ray_job.sh",
            "running ray job",
        ),
        Shell(
            "kubectl delete -f ray_cluster.yaml",
            "tear down ray cluster",
        ),
    ],
}


class Runner:
    def __init__(self, dry_run: bool = False, verbose: bool = False):
        self.dry_run = dry_run
        self.verbose = verbose
        self.cwd = os.getcwd()
        self.venv: str | None = None
        self.backgrounded = []

    def set_cwd(self, path: str):
        if os.path.isabs(path):
            self.cwd = path
        else:
            self.cwd = os.path.join(self.cwd, path)

    def activate_venv(self, path: str):
        self.venv = path

    def run_commands(
        self,
        commands: list[dict[str, str]],
        substitutions: dict[str, str] | None = None,
    ):
        if not substitutions:
            substitutions = {}

        substitutions["MY_DIR"] = MY_DIR

        for command in commands:
            match (self.dry_run, command):
                case (False, Shell(cmd, desc)):
                    self.run_shell_command(textwrap.dedent(cmd), desc, substitutions)

                case (True, Shell(cmd, desc)):
                    click.secho(f"[dry run] {desc} ...")
                    click.secho(f"    {cmd}", fg="yellow")

                case (False, BackgroundShell(cmd, desc)):
                    self.run_shell_command(
                        textwrap.dedent(cmd), desc, substitutions, background=True
                    )

                case (True, BackgroundShell(cmd, desc)):
                    click.secho(f"[dry run] {desc} ...")
                    click.secho(f"[backgrounding]    {cmd}", fg="yellow")

                case (False, Template(path, desc)):
                    click.secho(f"{desc} ...")
                    self.process_template(path, ".", substitutions)

                case (True, Template(path, desc)):
                    click.secho(f"[dry run] {desc} ...")
                    click.secho(f"    {path} subs:{substitutions}", fg="yellow")

                case (False, ChangeDir(path, desc)):
                    click.secho(f"{desc} ...")
                    self.set_cwd(path)

                case (True, ChangeDir(path, desc)):
                    click.secho(f"[dry run] {desc} ...")

                case (False, Venv(cmd, path, desc)):
                    self.run_shell_command(cmd, desc)
                    self.venv = os.path.abspath(path)

                case (True, Venv(cmd, path, desc)):
                    click.secho(f"[dry run] {desc} ...")

                case _:
                    raise Exception("Unhandled case in match.  Shouldn't happen")

    def run_shell_command(
        self,
        command: str,
        desc: str,
        substitutions: dict[str, str] | None = None,
        background: bool = False,
    ):
        click.secho(f"{desc} ...")
        if self.venv:
            venv_path = os.path.join(self.cwd, self.venv, "bin/activate")
            command = f"source {venv_path} && {command}"
        if substitutions:
            command = jinja2.Template(command).render(substitutions)

        if self.verbose:
            back = " background" if background else ""
            click.secho(f"[Running command{back}] {command}", fg="yellow")

        process = subprocess.Popen(
            command,
            shell=True,
            cwd=self.cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            executable="/bin/bash",
        )

        if background:
            self.backgrounded.append(process)
            return

        stdout, stderr = process.communicate()
        stdout = stdout.decode()
        stderr = stderr.decode()

        if process.returncode == 0:
            click.secho(f"    {stdout}", fg="green")
        else:
            click.secho(f"    stdout = {stdout}", fg="red")
            click.secho(f"    stderr = {stderr}", fg="red")
            click.secho(f"Error running command {command}")
            exit(1)

    def process_template(
        self, template_name: str, output_path: str, substitutions: dict[str, str] | None
    ):
        template_out = template_name[: template_name.index(".template")]
        output_path = os.path.join(output_path, template_out)
        template_path = os.path.join(MY_DIR, template_name)

        template = jinja2.Template(open(template_path).read())

        with open(output_path, "w") as f:
            f.write(template.render(substitutions))

    def __del__(self):
        for process in self.backgrounded:
            try:
                os.killpg(os.getpgid(process.pid), signal.SIGTERM)
            except Exception as e:
                print(f"Failed to kill process {process.pid}: {e}")
