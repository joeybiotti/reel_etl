# Use the official Airflow image (prebuilt)
FROM apache/airflow:2.6.3-python3.8

# Set working directory
WORKDIR /app

# Copy only the requirements file first (for caching)
COPY requirements.txt .

# Install additional dependencies **excluding Airflow**
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your files
COPY scripts/ scripts/
COPY data/ data/

# Define default entrypoint (if running ETL pipeline)
CMD ["python", "scripts/etl_pipeline.py"]
