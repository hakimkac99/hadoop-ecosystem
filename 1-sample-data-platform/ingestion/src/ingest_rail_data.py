import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime
from hdfs import InsecureClient
import os


RAIL_DATA_DOWNLOAD_URL = "https://publicdatafeeds.networkrail.co.uk/ntrod/CifFileAuthenticate?type=CIF_ALL_FULL_DAILY&day=toc-full"
HADOOP_NAMENODE_HOSTNAME = "http://hdfs-namenode:9870"

# download data from the rail data API - https://wiki.openraildata.com/index.php?title=SCHEDULE
basic_auth = HTTPBasicAuth(os.environ["RAIL_DATA_USERNAME"], os.environ["RAIL_DATA_PASSWORD"])
results = requests.get(url=RAIL_DATA_DOWNLOAD_URL, auth=basic_auth, timeout=600)
results.raise_for_status()

# Upload the file to HDFS
current_datetime = datetime.now().strftime("%Y/%m/%d/%H/%M")

HDFS_PATH = f"data_sources/{current_datetime}/rail_data.gz"

client = InsecureClient(url=HADOOP_NAMENODE_HOSTNAME)
with client.write(HDFS_PATH, overwrite=True) as writer:
    writer.write(results.content)

print(f"File successfully uploaded to HDFS. {HDFS_PATH}")
