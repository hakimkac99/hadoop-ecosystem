from typing import Dict, Optional

from helpers.hdfs import HDFSClient
from models.etl_table import ETLTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_date


class DateBronzeTable(ETLTable):
    def __init__(self, spark: SparkSession, hdfs: HDFSClient):
        super().__init__(
            spark=spark,
            hdfs=hdfs,
            name="date",
            storage_path="bronze/open_rail_data/date",
            partition_columns=["spark_job_creation_date"],
        )

    def extract_upstream(self, run_upstream: bool) -> Optional[Dict[str, DataFrame]]:
        self.logger.info("Start extracting dates from HDFS csv static file ...")

        # Extracting data from HDFS
        hdfs_base_url = "hdfs://hdfs-namenode:9000/user/root"
        df: DataFrame = self.spark.read.csv(
            f"{hdfs_base_url}/landing/open_rail_data/static_files/dates.csv",
            header=True,
        )
        return {
            "dates": df,
        }

    def transform(self, upstream_dataframes) -> DataFrame:
        dates_df = upstream_dataframes["dates"]

        dates_table_df: DataFrame = dates_df.withColumn(
            "spark_job_creation_date", current_date()
        )
        return dates_table_df
