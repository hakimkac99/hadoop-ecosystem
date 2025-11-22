from typing import Dict, Optional

from helpers.hdfs import HDFSClient
from models.etl_table import ETLTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from silver.dim_date import DimDateSilverTable
from silver.dim_location import DimLocationSilverTable
from silver.fact_scheduled_stops import FactScheduledStopsSilverTable


class StopsCountPerLocationPerDateGoldTable(ETLTable):
    def __init__(self, spark: SparkSession, hdfs: HDFSClient):
        super().__init__(
            spark=spark,
            hdfs=hdfs,
            name="stops_count_per_location_per_date",
            storage_path="gold/open_rail_data/stops_count_per_location_per_date",
            partition_columns=["schedule_date"],
            table_write_mode="overwrite",
            create_table_in_hive=True,
        )

    def extract_upstream(self, run_upstream: bool) -> Optional[Dict[str, DataFrame]]:
        fact_scheduled_stops_etl = FactScheduledStopsSilverTable(
            spark=self.spark, hdfs=self.hdfs
        )

        dim_date_etl = DimDateSilverTable(spark=self.spark, hdfs=self.hdfs)

        dim_location_etl = DimLocationSilverTable(spark=self.spark, hdfs=self.hdfs)

        if run_upstream:
            fact_scheduled_stops_etl.run_etl(run_upstream=run_upstream)
            dim_date_etl.run_etl(run_upstream=run_upstream)
            dim_location_etl.run_etl(run_upstream=run_upstream)

        # Extracting data from HDFS
        # TODO: incremental load based on last processed date
        return {
            "fact_scheduled_stops": fact_scheduled_stops_etl.read(),
            "dim_date": dim_date_etl.read(),
            "dim_location": dim_location_etl.read(),
        }

    def transform(self, upstream_dataframes: Dict[str, DataFrame]) -> DataFrame:
        fact_scheduled_stops = upstream_dataframes["fact_scheduled_stops"]
        dim_date = upstream_dataframes["dim_date"]
        dim_location = upstream_dataframes["dim_location"]

        dim_dates_columns = [
            col
            for col in dim_date.columns
            if col not in ["surrogate_key", "spark_job_creation_date"]
        ]

        stops_per_location_per_day = (
            fact_scheduled_stops.join(
                F.broadcast(dim_date),
                fact_scheduled_stops.schedule_date_key == dim_date.surrogate_key,
                how="inner",
            )
            .join(
                F.broadcast(dim_location),
                fact_scheduled_stops.location_key == dim_location.surrogate_key,
                how="inner",
            )
            .where(
                (
                    fact_scheduled_stops.public_arrival.isNotNull()
                    | fact_scheduled_stops.public_departure.isNotNull()
                    | F.col("pass").isNotNull()
                )
                & (dim_location.description.isNotNull())
            )
            .groupBy([*dim_dates_columns, "description"])
            .agg(F.count("*").alias("stops_count"))
            .withColumnRenamed("description", "location")
            .withColumnRenamed("date", "schedule_date")
        )

        return stops_per_location_per_day
