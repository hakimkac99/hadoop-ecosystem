from prefect import flow, get_run_logger
from prefect_flows.landing.schedules import ingest_schedules


@flow(name="Daily integration")
def run_daily():
    logger = get_run_logger()
    logger.info("Starting daily integration flow ...")

    # ingest schedules from API and export to HDFS
    ingest_schedules()
