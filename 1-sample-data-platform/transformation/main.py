# from bronze.schedules import SchedulesBronzeTable
from helpers.hdfs import HDFSClient
from pyspark.sql import SparkSession
from silver.fact_scheduled_stops import FactScheduledStopsSilverTable


def run_spark_transformations(spark: SparkSession, hdfs: HDFSClient):
    fact_scheduled_stops = FactScheduledStopsSilverTable(spark=spark, hdfs=hdfs)
    fact_scheduled_stops.run_etl(run_upstream=True)


if __name__ == "__main__":
    # Create a spark session
    spark: SparkSession = (
        SparkSession.builder.appName("Transform Rail data")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "hdfs://hdfs-namenode:9000/spark-logs")
        # .config("spark.executor.memory", "4g")       # executor heap
        # .config("spark.executor.cores", "2")         # cores per executor
        # .config("spark.executor.instances", "4")     # total executors
        # .config("spark.yarn.executor.memoryOverhead", "1024")
        .enableHiveSupport()
        .getOrCreate()
    )

    hdfs = HDFSClient(spark=spark)
    run_spark_transformations(spark, hdfs)
