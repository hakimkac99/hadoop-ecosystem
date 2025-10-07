from abc import ABC, abstractmethod
from functools import reduce
from typing import Dict, List, Optional

from helpers.hdfs import hdfs_replace_file

# from pyspark.logger import PySparkLogger
from pyspark.errors.exceptions.captured import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit


class ETLTable(ABC):
    def __init__(
        self,
        spark: SparkSession,
        name: str,
        storage_path: str,
        partition_columns: Optional[List[str]] = None,
        table_write_mode: str = "append",
    ):
        self.spark = spark
        self.name = name
        self.storage_path = storage_path
        self.partition_columns = partition_columns
        self.table_write_mode = table_write_mode

    @property
    def logger(self):
        return self.spark._jvm.org.apache.logging.log4j.LogManager.getLogger(
            "SparkLogger"
        )
        # return PySparkLogger.getLogger(name="ConsoleLogger")

    @abstractmethod
    def extract_upstream(self, run_upstream: bool):
        pass

    @abstractmethod
    def transform(self, upstream_dataframe: DataFrame):
        pass

    def load(self, table_data: DataFrame):
        self.logger.info(f"Loading {self.name} to HDFS : {self.storage_path} ...")

        destination_path = f"hdfs://hdfs-namenode:9000/user/root/{self.storage_path}"

        if self.table_write_mode == "append":
            table_data.write.parquet(
                path=destination_path,
                partitionBy=self.partition_columns,
                mode="append",
            )

        elif self.table_write_mode == "overwrite":
            # write to a temporary file then to the final destination to avoid FileNotFoundException when applying scd1

            # write to a temporary location
            temp_path = f"{destination_path}_tmp"
            table_data.write.parquet(
                path=temp_path,
                partitionBy=self.partition_columns,
                mode="overwrite",
            )

            hdfs_replace_file(
                spark=self.spark,
                source_file_path=temp_path,
                destination_file_path=destination_path,
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
        transformed_data = self.transform(
            self.extract_upstream(run_upstream=run_upstream)
        )
        self.load(transformed_data)
