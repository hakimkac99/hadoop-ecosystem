import glob
from pathlib import Path

from hdfs import InsecureClient
from prefect import flow, get_run_logger, task


class StaticFilesUploadToHDFS:
    """
    A reusable class that uploads static files to HDFS.
    HDFS authentication is not managed here.
    Args:
        source_files_path_pattern: source files path, wildcards are supported
        hadoop_namenode_hostname: Hadoop namenode hostname
        destination_folder_hdfs_path: folder where to upload files in HDFS
    """

    def __init__(
        self,
        source_files_path_pattern: str,
        hadoop_namenode_hostname: str,
        destination_hdfs_path: str,
    ):
        self.source_files_path_pattern = source_files_path_pattern
        self.destination_hdfs_path = destination_hdfs_path
        self.hadoop_namenode_hostname = hadoop_namenode_hostname

    @property
    def logger(self):
        return get_run_logger()

    @property
    def hdfs_client(self):
        return InsecureClient(url=self.hadoop_namenode_hostname)

    @flow(name="Upload static files to HDFS")
    def run(self):
        self.logger.info(
            "Start uploading static files to HDFS '%s' ...", self.destination_hdfs_path
        )

        files = [
            file
            for file in glob.glob(self.source_files_path_pattern, recursive=True)
            if Path(file).is_file()
        ]

        if not files:
            raise FileNotFoundError(
                f"No files found for pattern: {self.source_files_path_pattern}"
            )

        for file in files:
            self.upload_file_to_hdfs.with_options(name=f"Upload {file} to HDFS").submit(
                source_file_path=file
            )

        self.logger.info(
            "End uploading static files to HDFS '%s' ...", self.destination_hdfs_path
        )

    @task
    def upload_file_to_hdfs(self, source_file_path: str):
        self.logger.info(
            "Start uploading static file to HDFS '%s' ...", source_file_path
        )

        with open(source_file_path, "r") as f:
            with self.hdfs_client.write(
                hdfs_path=Path(self.destination_hdfs_path)
                / Path(source_file_path).name,
                overwrite=True,
            ) as writer:
                writer.write(f.read())

        self.logger.info("End uploading static file to HDFS '%s' ...", source_file_path)
