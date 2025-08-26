import logging
from abc import ABC, abstractmethod
from typing import List, Optional

from pydantic import BaseModel
from pyspark.sql import SparkSession


class Table(BaseModel):
    storage_path: str
    partition_columns: Optional[List[str]] = None


class ETLPipeline(ABC):
    def __init__(self, spark: SparkSession, name: str, output_table: Table):
        self.spark = spark
        self.name = name
        self.output_table = output_table

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
            path=f"hdfs://hdfs-namenode:9000/user/root/{self.output_table.storage_path}",
            partitionBy=self.output_table.partition_columns,
            mode="append",
        )

    def run(self):
        self.logger.info(f"Starting ETL Pipeline : {self.name}")
        transformed_data = self.transform(self.extract_upstream())
        self.load(transformed_data)
