from abc import ABC, abstractmethod
from functools import reduce
from typing import Dict, List, Optional

from helpers.hdfs import HDFSClient

# from pyspark.logger import PySparkLogger
from pyspark.errors.exceptions.captured import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit


class ETLTable(ABC):
    def __init__(
        self,
        spark: SparkSession,
        hdfs: HDFSClient,
        name: str,
        storage_path: str,
        partition_columns: Optional[List[str]] = None,
        table_write_mode: str = "append",
        primary_keys: Optional[List[str]] = None,
        create_table_in_hive: Optional[bool] = False,
    ):
        self.spark = spark
        self.hdfs = hdfs
        self.name = name
        self.storage_path = storage_path
        self.partition_columns = partition_columns
        self.table_write_mode = table_write_mode
        self.primary_keys = primary_keys
        self.create_table_in_hive = create_table_in_hive

    @property
    def logger(self):
        return self.spark._jvm.org.apache.logging.log4j.LogManager.getLogger(
            "SparkLogger"
        )
        # return PySparkLogger.getLogger(name="ConsoleLogger")

    @abstractmethod
    def extract_upstream(self, run_upstream: bool) -> Optional[Dict[str, DataFrame]]:
        pass

    @abstractmethod
    def transform(self, upstream_dataframes: Dict[str, DataFrame]):
        pass

    def load(self, table_data: DataFrame):
        self.logger.info(f"Loading {self.name} to HDFS : {self.storage_path} ...")

        destination_path = f"hdfs://hdfs-namenode:9000/user/root/{self.storage_path}"

        if self.create_table_in_hive:
            self.logger.info(f"Creating Hive table {self.name} ...")
            table_data.write.saveAsTable(
                name=f"data_warehouse.{self.name}",
                path=f"{destination_path}_tmp"
                if self.table_write_mode == "overwrite"
                else destination_path,
                partitionBy=self.partition_columns,
                mode=self.table_write_mode,
            )
            self.logger.info(f"Hive table {self.name} created successfully.")
        else:
            table_data.write.parquet(
                path=f"{destination_path}_tmp"
                if self.table_write_mode == "overwrite"
                else destination_path,
                partitionBy=self.partition_columns,
                mode=self.table_write_mode,
            )

        if self.table_write_mode == "overwrite":
            # write to a temporary file then to the final destination to avoid FileNotFoundException when applying scd1

            self.hdfs.replace_file(
                source_file_path=f"{destination_path}_tmp",
                destination_file_path=destination_path,
            )

            if self.create_table_in_hive:
                # Update Hive table location from tmp to final path
                self.spark.sql(
                    f"ALTER TABLE {self.name} SET LOCATION '{destination_path}'"
                )

    def read(self, partition_values: Optional[Dict[str, str]] = None):
        self.logger.info(
            f"Start reading {self.storage_path} using these partition values : {partition_values} ..."
        )

        try:
            df: DataFrame = self.spark.read.parquet(self.storage_path)
        except AnalysisException as e:
            self.logger.warn(e.getMessage())
            return None

        if partition_values:
            filter_conditions = [
                (col(partition_column).like(partition_values[partition_column]))
                if isinstance(partition_values[partition_column], str)
                else col(partition_column).isin(*partition_values[partition_column])
                if isinstance(partition_values[partition_column], list)
                else lit(True)
                for partition_column in partition_values
            ]
            partitions_filter = reduce(
                lambda condition_1, condition_2: condition_1 & condition_2,
                filter_conditions,
            )
            return df.filter(partitions_filter)

        return df

    def run_etl(self, run_upstream: bool = False):
        self.logger.info(f"Starting the ETL Pipeline : {self.name}")

        upstream_dataframes = self.extract_upstream(run_upstream=run_upstream)

        if upstream_dataframes:
            transformed_data = self.transform(upstream_dataframes)
            self.load(transformed_data)
        else:
            self.logger.warn(f"No data to load for '{self.storage_path}'")
