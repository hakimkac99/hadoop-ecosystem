from datetime import datetime

from prefect import flow
from prefect.blocks.system import Secret
from prefect_flows.models.api_extraction_to_hdfs import (
    APIConfiguration,
    APIExtractionToHDFS,
    APIMethod,
    BasicAuthConfiguration,
)


def get_rail_api_credentials():
    secret_block = Secret.load("rail-api-credentials")
    return secret_block.get()


@flow(name="Ingest Schedules data from API")
def ingest_schedules():
    api_credentials = get_rail_api_credentials()
    current_datetime = datetime.now().strftime("%Y/%m/%d/%H/%M")

    schedules_api_extraction_to_hdfs = APIExtractionToHDFS(
        api_configuration=APIConfiguration(
            base_url=(
                "https://publicdatafeeds.networkrail.co.uk/ntrod/CifFileAuthenticate"
                "?type=CIF_ALL_FULL_DAILY&day=toc-full"
            ),
            method=APIMethod.GET,
            auth_configuration=BasicAuthConfiguration(
                username=api_credentials["username"],
                password=api_credentials["password"],
            ),
        ),
        hadoop_namenode_hostname="http://hdfs-namenode:9870",
        destination_hdfs_path=f"data_sources/open_rail_data/{current_datetime}/rail_data.gz",
    )

    schedules_api_extraction_to_hdfs.run()
