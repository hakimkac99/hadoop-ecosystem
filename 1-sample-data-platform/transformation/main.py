# from bronze.schedules import SchedulesBronzeTable
from helpers.hdfs import HDFSClient
from pyspark.sql import SparkSession
from silver.dim_location import DimLocationSilverTable
from silver.dim_train_status import DimTrainStatusSilverTable


def run_spark_transformations(spark: SparkSession, hdfs: HDFSClient):
    train_status_table = DimTrainStatusSilverTable(spark=spark, hdfs=hdfs)
    train_status_table.run_etl(run_upstream=True)

    dim_location = DimLocationSilverTable(spark=spark, hdfs=hdfs)
    dim_location.run_etl(run_upstream=True)


if __name__ == "__main__":
    # Create a spark session
    spark: SparkSession = (
        SparkSession.builder.appName("Transform Rail data")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "hdfs://hdfs-namenode:9000/spark-logs")
        .getOrCreate()
    )

    hdfs = HDFSClient(spark=spark)
    run_spark_transformations(spark, hdfs)
