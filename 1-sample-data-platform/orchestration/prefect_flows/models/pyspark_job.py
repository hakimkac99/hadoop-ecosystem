from enum import Enum
from typing import List

from prefect import task
from prefect_shell import ShellOperation


class DeployMode(Enum):
    """
    Possible YARN deployment methods
    """

    CLUSTER = "cluster"
    CLIENT = "client"


class PySparkJob:
    """
    A helper class for submitting Pyspark jobs using the Prefect ShellOperation
    """

    def __init__(
        self,
        deploy_mode: DeployMode,
        py_files: List[str],
        working_dir: str = "spark_apps",
    ):
        self.deploy_mode = deploy_mode
        self.py_files = py_files
        self.working_dir = working_dir

    @task
    def submit(self):
        with ShellOperation(
            commands=[
                (
                    "spark-submit --master yarn "
                    f"--deploy-mode {self.deploy_mode.value} --py-files {' '.join(self.py_files)}"
                ),
            ],
            working_dir=self.working_dir,
        ) as submit_spark_job:
            # trigger runs the process in the background
            spark_job_process = submit_spark_job.trigger()

            # wait for the process to finish
            spark_job_process.wait_for_completion()
