from bronze.schedules import SchedulesBronzeTable
from pyspark.sql import SparkSession


def run_spark_transformations(spark: SparkSession):
    pipeline = SchedulesBronzeTable(spark=spark)
    pipeline.run()


if __name__ == "__main__":
    # Create a spark session
    spark: SparkSession = (
        SparkSession.builder.appName("Transform Rail data")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "hdfs://hdfs-namenode:9000/spark-logs")
        .getOrCreate()
    )
    run_spark_transformations(spark)
