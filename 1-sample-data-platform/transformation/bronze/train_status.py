from models.etl_table import ETLTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp


class TrainStatusBronzeTable(ETLTable):
    def __init__(self, spark: SparkSession):
        super().__init__(
            spark=spark,
            name="train_status",
            storage_path="bronze/open_rail_data/train_status",
            partition_columns=["spark_job_creation_timestamp"],
        )

    def extract_upstream(self) -> DataFrame:
        self.logger.info("Start extracting train status from HDFS csv static file ...")

        # Extracting data from HDFS
        hdfs_base_url = "hdfs://hdfs-namenode:9000/user/root"
        df: DataFrame = self.spark.read.csv(
            f"{hdfs_base_url}/landing/open_rail_data/static_files/train_status.csv",
            header=True,
        )
        return df

    def transform(self, upstream_dataframe) -> DataFrame:
        train_status_table_df: DataFrame = upstream_dataframe.withColumn(
            "spark_job_creation_timestamp", current_timestamp()
        )
        return train_status_table_df
