from datetime import date
from typing import Dict, Optional

from bronze.dates import DateBronzeTable
from helpers.hdfs import HDFSClient
from helpers.scd1 import scd1_merge
from models.etl_table import ETLTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import col, row_number


class DimDateSilverTable(ETLTable):
    def __init__(self, spark: SparkSession, hdfs: HDFSClient):
        super().__init__(
            spark=spark,
            hdfs=hdfs,
            name="dim_date",
            storage_path="silver/open_rail_data/dim_date",
            primary_keys=["date"],
            table_write_mode="overwrite",
            create_table_in_hive=True,
            scd_type=1,
        )

    def extract_upstream(self, run_upstream: bool) -> Optional[Dict[str, DataFrame]]:
        date_bronze_etl = DateBronzeTable(spark=self.spark, hdfs=self.hdfs)
        if run_upstream:
            date_bronze_etl.run_etl()

        # Extracting data from HDFS
        today = (date.today()).strftime("%Y-%m-%d")
        bronze_df = date_bronze_etl.read(
            partition_values={"spark_job_creation_date": f"{today}%"}
        )
        return {
            "bronze_date": bronze_df,
        }

    def transform(self, upstream_dataframes: Dict[str, DataFrame]) -> DataFrame:
        # deduplicate bronze table
        dedup_window = Window.orderBy(
            col("spark_job_creation_date").desc()
        ).partitionBy(*self.primary_keys)

        deduplicated_bronze_table_df = (
            upstream_dataframes["bronze_date"]
            .withColumn("row_number", row_number().over(dedup_window))
            .filter(col("row_number") == 1)
            .drop(col("row_number"))
        )

        existing_silver_table_df = self.read(read_scd1_copy=True)

        # apply scd1
        scd1_result = scd1_merge(
            df_source=deduplicated_bronze_table_df,
            df_target=existing_silver_table_df,
            primary_keys=self.primary_keys,
        )

        return scd1_result
