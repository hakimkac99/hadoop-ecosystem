from prefect import flow, get_run_logger


@flow
def my_flow(param_1: str = "value_1"):
    logger = get_run_logger()
    logger.info(f"Running Prefect flow with parameters : param_1: '{param_1}' ...")
