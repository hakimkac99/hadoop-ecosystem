from gold.stops_number_per_location_per_date import (
    StopsCountPerLocationPerDateGoldTable,
)
from helpers.hdfs import HDFSClient
from pyspark.sql import SparkSession


def run_spark_transformations(spark: SparkSession, hdfs: HDFSClient):
    StopsCountPerLocationPerDateGoldTable(spark=spark, hdfs=hdfs).run_etl(
        run_upstream=False
    )


if __name__ == "__main__":
    # Create a spark session
    spark: SparkSession = (
        SparkSession.builder.appName("Transform Rail data")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "hdfs://hdfs-namenode:9000/spark-logs")
        .config("spark.executor.memory", "1600m")
        .config("spark.executor.cores", "1")
        .enableHiveSupport()
        .getOrCreate()
    )

    hdfs = HDFSClient(spark=spark)
    run_spark_transformations(spark, hdfs)
