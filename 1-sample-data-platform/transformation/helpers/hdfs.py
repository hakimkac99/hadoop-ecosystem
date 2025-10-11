import re
from typing import Optional

from pyspark.sql import SparkSession


class HDFSClient:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.file_system = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jsc.hadoopConfiguration()
        )

    def replace_file(self, source_file_path: str, destination_file_path: str):
        """
        Replace the destination file with the source file in HDFS.
        If the destination exists, it is deleted before renaming the source.

        Args:
            source_file_path (str): Source HDFS path
            destination_file_path (str): Destination HDFS path to overwrite
        """
        source_file_path_obj = self.spark._jvm.org.apache.hadoop.fs.Path(
            source_file_path
        )
        destination_file_path_obj = self.spark._jvm.org.apache.hadoop.fs.Path(
            destination_file_path
        )

        # delete destination file if exists
        if self.file_system.exists(destination_file_path_obj):
            self.file_system.delete(destination_file_path_obj, True)

        # rename temp file to destination file
        self.file_system.rename(source_file_path_obj, destination_file_path_obj)

    def list_files(
        self, path: str, regex: Optional[str] = None, recursive: bool = False
    ):
        """
        List files in an HDFS directory, optionally filtering by regex pattern.

        Args:
            path (str): Base HDFS directory path.
            regex (str, optional): Regex pattern to filter file names.
            recursive (bool): Whether to search recursively.

        Returns:
            list[str]: List of matching HDFS file paths.
        """
        path_opj = self.spark._jvm.org.apache.hadoop.fs.Path(path)

        iterator = self.file_system.listFiles(path_opj, recursive)
        files = []
        pattern = re.compile(regex) if regex else None
        while iterator.hasNext():
            file_path = iterator.next().getPath().toString()
            if not pattern or pattern.search(file_path):
                files.append(file_path)
        return files
