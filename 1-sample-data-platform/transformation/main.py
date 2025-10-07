# from bronze.schedules import SchedulesBronzeTable
from pyspark.sql import SparkSession
from silver.train_status import TrainStatusSilverTable


def run_spark_transformations(spark: SparkSession):
    # schedules_table = SchedulesBronzeTable(spark=spark)
    train_status_table = TrainStatusSilverTable(spark=spark)
    train_status_table.run_etl(run_upstream=True)


if __name__ == "__main__":
    # Create a spark session
    spark: SparkSession = (
        SparkSession.builder.appName("Transform Rail data")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "hdfs://hdfs-namenode:9000/spark-logs")
        .getOrCreate()
    )
    run_spark_transformations(spark)
