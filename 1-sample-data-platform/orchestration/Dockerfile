# an image having both PySpark setup (including java) and Prefect.

FROM spark:3.5.7-scala2.12-java11-ubuntu


USER root

RUN set -ex; \
    apt-get update; \
    apt-get install -y software-properties-common wget; \
    add-apt-repository ppa:deadsnakes/ppa; \
    apt-get update; \
    apt-get install -y python3.11 python3.11-venv python3.11-distutils wget; \
    wget https://bootstrap.pypa.io/get-pip.py; \
    python3.11 get-pip.py; \
    rm get-pip.py; \
    rm -rf /var/lib/apt/lists/*


# Copy Prefect flows
COPY prefect_flows /opt/prefect/prefect_flows
COPY requirements.txt /opt/prefect/requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /opt/prefect/requirements.txt

# Postgresql jdbc driver for the HIVE metastore connection
COPY drivers /opt/spark/jars/