# Use the official Airflow image (prebuilt)
FROM apache/airflow:2.6.3-python3.8

# Set working directory
WORKDIR /workspace

# Copy dependencies first (optimizes caching)
COPY requirements.txt .
COPY config.toml .

# Install additional dependencies excluding Airflow (already in the base image)
RUN pip install --no-cache-dir -r requirements.txt && pip install --no-cache-dir tomli python-json-logger

# Copy scripts and project files AFTER dependencies (reduces unnecessary rebuilds)
COPY scripts/ /workspace/scripts/

# Set up logging for better debugging inside the container
RUN mkdir -p /workspace/logs

# Default entry point for running the ETL pipeline
ENTRYPOINT ["python", "scripts/etl_pipeline.py"]
