from models.pyspark_job import DeployMode, PySparkJob
from prefect import flow, get_run_logger
from prefect_flows.landing.schedules import ingest_schedules
from prefect_flows.landing.static_files import upload_static_files_to_hdfs


@flow(name="Daily integration")
def run_daily():
    logger = get_run_logger()
    logger.info("Starting daily integration flow ...")

    # upload static files to HDFS
    upload_static_files_to_hdfs()

    # ingest schedules from API and load to HDFS
    ingest_schedules()

    logger.info("Submitting PySpark job to the Hadoop cluster ...")
    # transform schedules
    PySparkJob(
        deploy_mode=DeployMode.CLIENT,
        py_files=["transformation.zip", "transformation/main.py"],
    ).submit.with_options(name="Submit transformation PySpark job")()
