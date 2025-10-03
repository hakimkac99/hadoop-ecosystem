import logging
from abc import ABC, abstractmethod
from functools import reduce
from typing import Dict, List, Optional

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
        return logging.getLogger()

    @abstractmethod
    def extract_upstream(self):
        pass

    @abstractmethod
    def transform(self, upstream_dataframe):
        pass

    def load(self, table_data):
        table_data.write.parquet(
            path=f"hdfs://hdfs-namenode:9000/user/root/{self.storage_path}",
            partitionBy=self.partition_columns,
            mode=self.table_write_mode,
        )

    def read(self, partition_values: Optional[Dict[str, str]] = None):
        try:
            df: DataFrame = self.spark.read.parquet(self.storage_path)
        except AnalysisException as e:
            self.logger.warning(e)
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

    def run(self):
        self.logger.info(f"Starting ETL Pipeline : {self.name}")
        transformed_data = self.transform(self.extract_upstream())
        self.load(transformed_data)
