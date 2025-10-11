from datetime import date

from helpers.hdfs import HDFSClient
from models.etl_table import ETLTable
from pyspark.errors.exceptions.captured import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp


class LocationsBronzeTable(ETLTable):
    def __init__(self, spark: SparkSession, hdfs: HDFSClient):
        super().__init__(
            spark=spark,
            hdfs=hdfs,
            name="location",
            storage_path="bronze/open_rail_data/location",
            partition_columns=["spark_job_creation_timestamp"],
        )

    def extract_upstream(self, run_upstream: bool) -> DataFrame | None:
        self.logger.info("Start extracting locations from HDFS ...")

        # Extracting data from HDFS
        hdfs_base_url = "hdfs://hdfs-namenode:9000/user/root"
        file_name = "rail_data.gz"

        today = (date.today()).strftime("%Y/%m/%d")

        try:
            df: DataFrame = (
                self.spark.read.option("recursiveFileLookup", "true")
                .option("pathGlobFilter", file_name)
                .json(f"{hdfs_base_url}/landing/open_rail_data/{today}")
            )

            return df
        except AnalysisException as e:
            self.logger.warn(f"'{file_name}' file failed to read : {e.getMessage()}")
            return None

    def transform(self, upstream_dataframe) -> DataFrame:
        locations_table_df: DataFrame = (
            upstream_dataframe.filter(col("TiplocV1").isNotNull())
            .select(col("TiplocV1.*"))
            .withColumn("spark_job_creation_timestamp", current_timestamp())
        )

        return locations_table_df
