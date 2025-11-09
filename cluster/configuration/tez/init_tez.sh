#!/bin/bash
set -e


echo "Checking if /apps/tez exists..."
if hadoop fs -test -d /apps/tez; then
  echo "/apps/tez already exists in HDFS. Skipping upload."
else
  echo "Uploading Tez libraries to /apps/tez..."
  hadoop fs -mkdir -p /apps/tez
  hadoop fs -copyFromLocal /usr/local/tez /apps
  echo "Tez uploaded successfully."
fi