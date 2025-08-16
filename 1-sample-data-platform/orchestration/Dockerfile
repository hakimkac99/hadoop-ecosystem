# an image having both PySpark setup (including java) and Prefect.

FROM apache/spark-py:latest

USER root

# Install Prefect
RUN pip install --no-cache-dir prefect==3.4.12

# Copy your flows
COPY prefect_flows /opt/prefect/prefect_flows
COPY requirements.txt /opt/prefect/requirements.txt

# Install additional Python dependencies
RUN pip install --no-cache-dir -r /opt/prefect/requirements.txt
