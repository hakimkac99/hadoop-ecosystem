from datetime import Dict, date
from typing import Optional

from bronze.train_status import TrainStatusBronzeTable
from helpers.hdfs import HDFSClient
from helpers.scd1 import scd1_merge
from models.etl_table import ETLTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import col, row_number


class DimTrainStatusSilverTable(ETLTable):
    def __init__(self, spark: SparkSession, hdfs: HDFSClient):
        super().__init__(
            spark=spark,
            hdfs=hdfs,
            name="dim_train_status",
            storage_path="silver/open_rail_data/dim_train_status",
            partition_columns=["spark_job_creation_timestamp"],
            primary_keys=["code"],
            table_write_mode="overwrite",
        )

    def extract_upstream(self, run_upstream: bool) -> Optional[Dict[str, DataFrame]]:
        train_status_bronze_etl = TrainStatusBronzeTable(
            spark=self.spark, hdfs=self.hdfs
        )
        if run_upstream:
            train_status_bronze_etl.run_etl()

        # Extracting data from HDFS
        today = (date.today()).strftime("%Y-%m-%d")
        bronze_df = train_status_bronze_etl.read(
            partition_values={"spark_job_creation_timestamp": f"{today}%"}
        )
        return {"bronze_train_status": bronze_df}

    def transform(self, upstream_dataframes: Dict[str, DataFrame]) -> DataFrame:
        # deduplicate bronze table
        dedup_window = Window.orderBy(
            col("spark_job_creation_timestamp").desc()
        ).partitionBy(*self.primary_keys)

        deduplicated_bronze_table_df = (
            upstream_dataframes["bronze_train_status"]
            .withColumn("row_number", row_number().over(dedup_window))
            .filter(col("row_number") == 1)
            .drop(col("row_number"))
        )

        existing_silver_table_df = self.read()

        # apply scd1
        scd1_result = scd1_merge(
            df_source=deduplicated_bronze_table_df,
            df_target=existing_silver_table_df,
            primary_keys=self.primary_keys,
        )

        return scd1_result
