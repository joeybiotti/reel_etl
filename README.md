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

---

## Project Structure

```
reel_etl/
├── app/                   # FastAPI app
├── data/                  # Raw and processed CSV files
├── logs/                  # JSON and text log files
├── notebooks/             # Exploratory Jupyter notebooks
├── reports/               # ydata-profiling HTML report
├── scripts/               # Utility scripts
├── tests/                 # Unit tests
├── movies.db              # SQLite database
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

3. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

4. Run the ETL pipeline:

   ```bash
   python scripts/etl_pipeline.py
   ```

5. (Optional) Launch the FastAPI server:
   ```bash
   uvicorn app.main:app --reload
   ```

---

## Quick Start

- Run ETL: Clean and load your movie dataset.
- Explore: Open `notebooks/explore_movies.ipynb`.
- Profile Data: View `reports/movie_data_profile.html`.
- Query API: Visit `http://127.0.0.1:8000/docs` to try the API endpoints.

---

## Tech Stack

- Python 3.8+
- Pandas
- FastAPI
- SQLite
- pytest
- ydata-profiling
- Seaborn and Matplotlib (for visuals)

---

## License

This project is licensed under the MIT License.

---

## Acknowledgements

Inspired by classic data engineering and analytics workflows.  
Built by [@joeybiotti](https://github.com/joeybiotti).
