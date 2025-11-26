from pyspark.sql import SparkSession
from pyspark.testing.utils import assertDataFrameEqual


def test_scd_1():
    spark = SparkSession.builder.appName("Testing PySpark Example").getOrCreate()

    df1 = spark.createDataFrame(
        data=[("1", 1000), ("2", 3000)], schema=["id", "amount"]
    )
    df2 = spark.createDataFrame(
        data=[("1", 1000), ("2", 3000)], schema=["id", "amount"]
    )
    assertDataFrameEqual(df1, df2)  # pass, DataFrames are identical
