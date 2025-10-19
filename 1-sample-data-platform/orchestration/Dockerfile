# an image having both PySpark setup (including java) and Prefect.

FROM apache/spark:4.0.0-python3

USER root

# Install Prefect
RUN pip install --no-cache-dir prefect==3.4.12

# Copy your flows
COPY prefect_flows /opt/prefect/prefect_flows
COPY requirements.txt /opt/prefect/requirements.txt

# Install additional Python dependencies
RUN pip install --no-cache-dir -r /opt/prefect/requirements.txt

# postgresql jdbc driver for the HIVE metastore connection
COPY drivers /opt/spark/jars/