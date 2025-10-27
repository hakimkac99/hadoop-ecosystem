from datetime import date
from typing import Dict, Optional

from bronze.schedules import SchedulesBronzeTable
from helpers.hdfs import HDFSClient
from models.etl_table import ETLTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from silver.dim_date import DimDateSilverTable
from silver.dim_location import DimLocationSilverTable
from silver.dim_train_status import DimTrainStatusSilverTable


class FactScheduledStopsSilverTable(ETLTable):
    def __init__(self, spark: SparkSession, hdfs: HDFSClient):
        super().__init__(
            spark=spark,
            hdfs=hdfs,
            name="fact_scheduled_stops",
            storage_path="silver/open_rail_data/fact_scheduled_stops",
            partition_columns=["schedule_date"],
            table_write_mode="overwrite",
            create_table_in_hive=True,
        )

    def extract_upstream(self, run_upstream: bool) -> Optional[Dict[str, DataFrame]]:
        schedule_bronze_etl = SchedulesBronzeTable(spark=self.spark, hdfs=self.hdfs)

        dim_date_silver_etl = DimDateSilverTable(spark=self.spark, hdfs=self.hdfs)

        dim_train_status_silver_etl = DimTrainStatusSilverTable(
            spark=self.spark, hdfs=self.hdfs
        )

        dim_location_silver_etl = DimLocationSilverTable(
            spark=self.spark, hdfs=self.hdfs
        )

        if run_upstream:
            schedule_bronze_etl.run_etl(run_upstream=run_upstream)
            dim_date_silver_etl.run_etl(run_upstream=run_upstream)
            dim_train_status_silver_etl.run_etl(run_upstream=run_upstream)
            dim_location_silver_etl.run_etl(run_upstream=run_upstream)

        # Extracting data from HDFS
        today = (date.today()).strftime("%Y-%m-%d")
        return {
            "bronze_schedule": schedule_bronze_etl.read(
                partition_values={"spark_job_creation_date": today}
            ),
            "dim_date": dim_date_silver_etl.read(),
            "dim_train_status": dim_train_status_silver_etl.read(),
            "dim_location": dim_location_silver_etl.read(),
        }

    def transform(self, upstream_dataframes: Dict[str, DataFrame]) -> DataFrame:
        bronze_schedule = upstream_dataframes["bronze_schedule"]
        dim_date = upstream_dataframes["dim_date"]
        dim_train_status = upstream_dataframes["dim_train_status"]
        dim_location = upstream_dataframes["dim_location"]

        # Generate scheduled valid dates, from start to end date, filtering by days runs
        schedule_with_dates = bronze_schedule.withColumn(
            "scheduled_dates",
            F.sequence(
                F.to_date("schedule_start_date", "yyyy-MM-dd"),
                F.to_date("schedule_end_date", "yyyy-MM-dd"),
                F.expr("interval 1 day"),
            ),
        ).withColumn(
            "valid_dates",
            F.filter(
                "scheduled_dates",
                lambda d: F.substring(
                    F.col("schedule_days_runs"), (F.dayofweek(d) + 5) % 7 + 1, 1
                )
                == 1,
            ),
        )

        # Explode to daily-level records
        daily_schedule = schedule_with_dates.withColumn(
            "schedule_date", F.explode("valid_dates")
        ).select(
            "CIF_train_uid",
            "CIF_stp_indicator",
            "train_status",
            "atoc_code",
            "schedule_segment",
            "schedule_date",
        )

        # Explode schedule stops
        schedule_stops = daily_schedule.withColumn(
            "location", F.explode("schedule_segment.schedule_location")
        ).select(
            "CIF_train_uid",
            "CIF_stp_indicator",
            "train_status",
            "atoc_code",
            "schedule_date",
            F.col("location.tiploc_code").alias("tiploc_code"),
            F.col("location.platform").alias("platform"),
            F.col("location.location_type").alias("location_type"),
            F.col("location.public_arrival").alias("public_arrival"),
            F.col("location.public_departure").alias("public_departure"),
            F.col("location.pass").alias("pass"),
        )

        # Join with dim_date, dim_train_status and dim_location
        fact_scheduled_stops = (
            schedule_stops.join(
                F.broadcast(dim_date),
                daily_schedule.schedule_date == dim_date.date,
                how="inner",
            )
            .join(F.broadcast(dim_location), on="tiploc_code", how="inner")
            .join(
                F.broadcast(dim_train_status),
                daily_schedule.train_status == dim_train_status.code,
                how="inner",
            )
            .select(
                "CIF_train_uid",
                "schedule_date",
                "CIF_stp_indicator",
                "atoc_code",
                "platform",
                "location_type",
                "public_arrival",
                "public_departure",
                "pass",
                dim_train_status.surrogate_key.alias("train_status_key"),
                dim_date.surrogate_key.alias("schedule_date_key"),
                dim_location.surrogate_key.alias("location_key"),
            )
        )

        return fact_scheduled_stops
