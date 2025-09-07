from prefect_flows.models.static_files_to_hdfs import StaticFilesUploadToHDFS


def upload_static_files_to_hdfs():
    static_files_upload_to_hdfs = StaticFilesUploadToHDFS(
        source_files_path_pattern="./ingestion/static_files/**/*",
        hadoop_namenode_hostname="http://hdfs-namenode:9870",
        destination_hdfs_path="landing/open_rail_data/static_files",
    )

    static_files_upload_to_hdfs.run()
