from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").\
appName("Read CSV file").\
config("spark.driver.bindAddress","localhost").\
config("spark.ui.port","4040").\
getOrCreate()

# Read CSV file from HDFS
df = spark.read.csv("hdfs://hdfs-namenode:9000/warehouse/users.csv", header=True, inferSchema=True)
df.show()

input("Press enter to terminate")
