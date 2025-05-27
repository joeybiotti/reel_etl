# Reel ETL

A Python-based data pipeline that extracts, transforms, and loads movie metadata into SQLite, with robust logging, validation, exploratory analysis, and an API for querying movie information.

## Features

- **ETL Pipeline** – Extracts movie data from CSV, transforms it, and loads it into a database.
- **Logging** – Console + JSON file logging with rotation.
- **Data Validation** – Ensures data integrity and correctness.
- **Testing** – Unit tests using `pytest`.
- **Exploration** – Jupyter notebook & `ydata-profiling` report.
- **API** – FastAPI app for querying movies by title and director.
- **SQL Queries** – Pre-built analytical queries for insights.

## Project Structure

```sh
reel_etl/
├── app/            # FastAPI app
├── data/           # Raw and processed CSV files
├── logs/           # JSON and text log files
├── notebooks/      # Exploratory Jupyter notebooks
├── reports/        # Data profiling report
├── scripts/        # ETL pipeline and utilities
├── tests/          # Unit tests
├── movies.db       # SQLite database
├── run_api.sh      # Script to launch API
├── run_tests.sh    # Script to run tests
├── requirements.txt # Python dependencies
├── .gitignore
├── README.md
```

## Installation

1. **Clone the repository**

   ```sh
   git clone https://github.com/joeybiotti/reel_etl.git
   cd reel_etl
   ```

2. **Create a virtual environment**

   ```sh
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**

   ```sh
   pip install -r requirements.txt
   ```

4. **Run the ETL pipeline**
   ```sh
   python scripts/etl_pipeline.py
   ```

## Running the Application

### Start the API

```sh
./run_api.sh
```

Access:

- **API** – [http://127.0.0.1:8000](http://127.0.0.1:8000)
- **Swagger UI** – [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)
- **ReDoc** – [http://127.0.0.1:8000/redoc](http://127.0.0.1:8000/redoc)

### Run ETL Pipeline with Airflow

```sh
export AIRFLOW_HOME=$(pwd)/airflow
airflow db init
airflow webserver -p 8080
airflow scheduler
```

Trigger the DAG via [http://localhost:8080](http://localhost:8080).

## Running Tests

```sh
./run_tests.sh
```

If needed:

```sh
pip install pytest
```

## Quick Start Summary

| Action                    | Command                                |
| ------------------------- | -------------------------------------- |
| **Run ETL**               | `python scripts/etl_pipeline.py`       |
| **Explore Data**          | Open `notebooks/explore_movies.ipynb`  |
| **View Profiling Report** | Open `reports/movie_data_profile.html` |
| **Launch API**            | `./run_api.sh`                         |
| **Run Tests**             | `./run_tests.sh`                       |

## Tech Stack

- **Python 3.8+**
- **Pandas, FastAPI, SQLite**
- **pytest, ydata-profiling**
- **Seaborn, Matplotlib**

## License

MIT License

## Acknowledgements

Inspired by classic data engineering workflows.  
Built by [@joeybiotti](https://github.com/joeybiotti).
