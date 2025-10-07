from pyspark.sql import SparkSession


def hdfs_replace_file(
    spark: SparkSession, source_file_path: str, destination_file_path: str
):
    """
    Replace the destination file with the source file in HDFS.
    If the destination exists, it is deleted before renaming the source.

    Args:
        spark (SparkSession): Active Spark session
        source_file_path (str): Source HDFS path
        destination_file_path (str): Destination HDFS path to overwrite
    """
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jsc.hadoopConfiguration()
    )
    source_file_path_obj = spark._jvm.org.apache.hadoop.fs.Path(source_file_path)
    destination_file_path_obj = spark._jvm.org.apache.hadoop.fs.Path(
        destination_file_path
    )

    # delete destination file if exists
    if fs.exists(destination_file_path_obj):
        fs.delete(destination_file_path_obj, True)

    # rename temp file to destination file
    fs.rename(source_file_path_obj, destination_file_path_obj)
