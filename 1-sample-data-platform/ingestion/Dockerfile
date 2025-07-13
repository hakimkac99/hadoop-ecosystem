FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy ingestion scripts and requirements

COPY ./requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src .

CMD ["python", "ingest_rail_data.py"]