from typing import Dict, Optional

from helpers.hdfs import HDFSClient
from models.etl_table import ETLTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_date


class TrainStatusBronzeTable(ETLTable):
    def __init__(self, spark: SparkSession, hdfs: HDFSClient):
        super().__init__(
            spark=spark,
            hdfs=hdfs,
            name="train_status",
            storage_path="bronze/open_rail_data/train_status",
            partition_columns=["spark_job_creation_date"],
        )

    def extract_upstream(self, run_upstream: bool) -> Optional[Dict[str, DataFrame]]:
        self.logger.info("Start extracting train status from HDFS csv static file ...")

        # Extracting data from HDFS
        hdfs_base_url = "hdfs://hdfs-namenode:9000/user/root"
        df: DataFrame = self.spark.read.csv(
            f"{hdfs_base_url}/landing/open_rail_data/static_files/train_status.csv",
            header=True,
        )
        return {
            "train_status": df,
        }

    def transform(self, upstream_dataframes) -> DataFrame:
        train_status_table_df: DataFrame = upstream_dataframes[
            "train_status"
        ].withColumn("spark_job_creation_date", current_date())
        return train_status_table_df
