# Reel ETL

Reel ETL is a Python-based data pipeline that extracts, transforms, and loads movie metadata into a SQLite database.  
It includes robust logging, data validation, exploratory analysis, and an API for querying movie information.

---

## Features

- ETL Pipeline: Extracts movie data from CSV, transforms it, and loads it into a database.
- Logging: Console + JSON file logging with rotation.
- Data Validation: Ensures critical fields and data types are correct.
- Testing: Unit tests using `pytest`.
- Exploration: Jupyter notebook and ydata-profiling report.
- API: FastAPI app for querying movies by title and director.
- SQL Queries: Pre-built analytical queries for insights.
- Command-Line Scripts: Scripts to run the API server and tests easily.

---

## Project Structure

```
reel_etl/
├── app/                   # FastAPI app
├── data/                  # Raw and processed CSV files
├── logs/                  # JSON and text log files
├── notebooks/             # Exploratory Jupyter notebooks
├── reports/               # ydata-profiling HTML report
├── scripts/               # Utility scripts and ETL pipeline
├── tests/                 # Unit tests
├── movies.db              # SQLite database
├── run_api.sh             # Bash script to launch the API server
├── run_tests.sh           # Bash script to run tests
├── requirements.txt       # Python dependencies
├── .gitignore
├── README.md
```

---

## Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/joeybiotti/reel_etl.git
   cd reel_etl
   ```

2. Create a virtual environment:

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies (including Airflow and all ETL/analysis requirements):

   ```bash
   pip install -r requirements.txt
   ```

4. Run the ETL pipeline:

   ```bash
   python scripts/etl_pipeline.py
   ```

---

## Running the Application

### Start the API

To launch the FastAPI server:

```bash
./run_api.sh
```

The API will be available at [http://127.0.0.1:8000](http://127.0.0.1:8000).

You can explore interactive documentation at:

- Swagger UI: [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)
- ReDoc: [http://127.0.0.1:8000/redoc](http://127.0.0.1:8000/redoc)

---

### Run the ETL Pipeline with Airflow

If you want to orchestrate the ETL pipeline using Apache Airflow:

1. **Initialize Airflow (first time only):**

   ```bash
   export AIRFLOW_HOME=$(pwd)/airflow
   airflow db init
   airflow users create \
     --username admin \
     --firstname Admin \
     --lastname User \
     --role Admin \
     --email admin@example.com \
     --password admin
   ```

2. **Start the Airflow webserver and scheduler:**

   ```bash
   airflow webserver -p 8080
   airflow scheduler
   ```

3. **Access the Airflow UI:**  
   Go to [http://localhost:8080](http://localhost:8080) and trigger the ETL DAG.

---

### Run the Tests

To execute all unit tests:

```bash
./run_tests.sh
```

Make sure `pytest` is installed:

```bash
pip install pytest
```

---

## Quick Start Summary

- **ETL Pipeline**: `python scripts/etl_pipeline.py`
- **Data Exploration**: Open `notebooks/explore_movies.ipynb`
- **Data Profiling**: View `reports/movie_data_profile.html`
- **Launch API**: `./run_api.sh`
- **Run Tests**: `./run_tests.sh`

---

## Tech Stack

- Python 3.8+
- Pandas
- FastAPI
- SQLite
- pytest
- ydata-profiling
- Seaborn and Matplotlib

---

## License

This project is licensed under the MIT License.

---

## Acknowledgements

Inspired by classic data engineering and analytics workflows.  
Built by [@joeybiotti](https://github.com/joeybiotti).
