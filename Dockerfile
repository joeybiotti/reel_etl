# Use the official Airflow image (prebuilt)
FROM apache/airflow:2.6.3-python3.8

# Set working directory first to ensure consistent paths
WORKDIR /workspace

# Copy only the requirements file first (for caching)
COPY requirements.txt .
COPY config.toml .

# Install additional dependencies **excluding Airflow**, since it's already in the base image
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install tomli

# Copy the rest of your project files **after dependencies are installed** to speed up rebuilds
COPY scripts/ scripts/
COPY data/ data/

# Define default command to run the ETL pipeline
CMD ["python", "scripts/etl_pipeline.py"]
