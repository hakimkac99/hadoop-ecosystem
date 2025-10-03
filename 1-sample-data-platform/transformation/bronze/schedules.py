from datetime import date

from models.etl_table import ETLTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp


class SchedulesBronzeTable(ETLTable):
    def __init__(self, spark: SparkSession):
        super().__init__(
            spark=spark,
            name="schedule",
            storage_path="bronze/open_rail_data/schedule",
            partition_columns=["spark_job_creation_timestamp"],
        )

    def extract_upstream(self) -> DataFrame:
        self.logger.info("Start extracting schedules from HDFS ...")

        # Extracting data from HDFS
        hdfs_base_url = "hdfs://hdfs-namenode:9000/user/root"
        today = (date.today()).strftime("%Y/%m/%d")
        df: DataFrame = self.spark.read.json(
            f"{hdfs_base_url}/landing/open_rail_data/{today}/*/*/rail_data.gz"
        )

        return df

    def transform(self, upstream_dataframe) -> DataFrame:
        schedules_table_df: DataFrame = (
            upstream_dataframe.filter(col("JsonScheduleV1").isNotNull())
            .select(col("JsonScheduleV1.*"))
            .withColumn("spark_job_creation_timestamp", current_timestamp())
        )

        return schedules_table_df
