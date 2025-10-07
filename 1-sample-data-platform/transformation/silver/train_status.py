from datetime import date

from bronze.train_status import TrainStatusBronzeTable
from helpers.scd1 import scd1_merge
from models.etl_table import ETLTable
from pyspark.sql import DataFrame, SparkSession


class TrainStatusSilverTable(ETLTable):
    def __init__(self, spark: SparkSession):
        super().__init__(
            spark=spark,
            name="train_status",
            storage_path="silver/open_rail_data/train_status",
            partition_columns=["spark_job_creation_timestamp"],
            table_write_mode="overwrite",
        )

    def extract_upstream(self, run_upstream: bool) -> DataFrame:
        train_status_bronze_etl = TrainStatusBronzeTable(spark=self.spark)
        if run_upstream:
            train_status_bronze_etl.run_etl()

        # Extracting data from HDFS
        today = (date.today()).strftime("%Y-%m-%d")
        bronze_df = train_status_bronze_etl.read(
            partition_values={"spark_job_creation_timestamp": f"{today}%"}
        )
        return bronze_df

    def transform(self, upstream_dataframe: DataFrame) -> DataFrame:
        # deduplicate bronze table
        deduplicated_bronze_table_df = upstream_dataframe.dropDuplicates(["code"])

        existing_silver_table_df = self.read()

        if existing_silver_table_df:
            # apply scd1
            scd1_result = scd1_merge(
                df_source=deduplicated_bronze_table_df,
                df_target=existing_silver_table_df,
                primary_keys=["code"],
            )
            return scd1_result
        return deduplicated_bronze_table_df
