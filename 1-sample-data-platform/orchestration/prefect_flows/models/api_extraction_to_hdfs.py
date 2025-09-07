from enum import Enum
from typing import Any, Optional

import requests
from hdfs import InsecureClient
from prefect import flow, get_run_logger, task
from pydantic import BaseModel
from requests.auth import HTTPBasicAuth


class APIMethod(Enum):
    """
    Possible API methods
    """

    GET = "get"
    POST = "post"


class BasicAuthConfiguration(BaseModel):
    """
    Basic API auth Configuration
    """

    username: str
    password: str


class APIConfiguration(BaseModel):
    """
    API configuration for fetching data
    Args:
        base_url: base url to call
        method: API method for calling the API
        auth_configuration: authentication configuration in order to access the API,
                            only Basic Auth is currently possible.
    """

    base_url: str
    method: Optional[APIMethod] = APIMethod.GET
    auth_configuration: Optional[BasicAuthConfiguration] = None
    timeout: Optional[int] = 600
    body: Optional[dict] = None


class APIExtractionToHDFS:
    """
    A reusable class that fetches data from an API and writes the output to HDFS in file format.
    HDFS authentication is not managed here.
    Args:
        api_configuration: details about the API
        hadoop_namenode_hostname: Hadoop namenode hostname
        destination_hdfs_path: path where to save the output file in HDFS
    """

    def __init__(
        self,
        api_configuration: APIConfiguration,
        hadoop_namenode_hostname: str,
        destination_hdfs_path: str,
    ):
        self.api_configuration = api_configuration
        self.destination_hdfs_path = destination_hdfs_path
        self.hadoop_namenode_hostname = hadoop_namenode_hostname

    @property
    def logger(self):
        return get_run_logger()

    @task(name="Fetch data from the API")
    def extract(self) -> str:
        """
        This methods calls the API then return the result as a string
        """
        self.logger.info("Starting fetching data from the API ...")

        match self.api_configuration.auth_configuration:
            case BasicAuthConfiguration():
                auth_config = HTTPBasicAuth(
                    username=self.api_configuration.auth_configuration.username,
                    password=self.api_configuration.auth_configuration.password,
                )
            case _:
                auth_config = None

        match self.api_configuration.method:
            case APIMethod.GET:
                result = requests.get(
                    url=self.api_configuration.base_url,
                    auth=auth_config,
                    timeout=self.api_configuration.timeout,
                )
            case APIMethod.POST:
                result = requests.post(
                    url=self.api_configuration.base_url,
                    auth=auth_config,
                    timeout=self.api_configuration.timeout,
                    data=self.api_configuration.body,
                )
            case _:
                result = None

        result.raise_for_status()

        self.logger.info("Data successfully fetched from the API.")

        return result.content

    @task(name="Load result file to HDFS")
    def load(self, content: bytes | Any):
        """
        This method creates a file in HDFS
        """
        self.logger.info(
            "Start uploading file to HDFS '%s' ...", self.destination_hdfs_path
        )

        client = InsecureClient(url=self.hadoop_namenode_hostname)
        with client.write(self.destination_hdfs_path, overwrite=True) as writer:
            writer.write(content)

        self.logger.info(
            "File successfully uploaded to HDFS. '%s'", self.destination_hdfs_path
        )

    @flow(name="API extraction")
    def run(self):
        """
        This method runs the API extraction and loading process
        """
        content = self.extract()
        self.load(content)
