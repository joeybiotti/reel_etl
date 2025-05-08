# reel-etl: Building a Movie Data Pipeline

This project extracts, transforms, and loads (ETL) movie data from the IMDB 5000 Movie Dataset into a clean SQLite database.  
It focuses on real-world data engineering practices, using lightweight tooling and reproducible scripts.

## Dataset

- **Source:** [IMDB 5000 Movie Dataset on Kaggle](https://www.kaggle.com/datasets/carolzhangdc/imdb-5000-movie-dataset)
- **Contents:** Movies, genres, directors, revenues, release years, and more.

## Tech Stack

- Python 3.x
- SQLite3 (local database)
- Pandas (for data manipulation)
- Python Logging (for monitoring ETL processes)
- SQL (for querying and modeling)

## Project Structure

/data/ # Raw and cleaned CSVs
/scripts/ # ETL scripts and utilities (logging, decorators)
/sql/ # SQL queries and schema creation
/notebooks/ # (Optional) Jupyter notebooks for exploration
README.md
requirements.txt

## Goals

- Design a basic database schema for movies.
- Clean messy or missing data fields.
- Load structured data into SQLite.
- Write example SQL queries for analysis:
  - Top 10 highest-grossing movies
  - Average IMDb score by genre
  - Directors with the most movies

## How to Run

1. Clone the repo
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Run the ETL script:
   ```bash
   python scripts/etl_pipeline.py
   ```
4. Explore the SQLite database with your favorite tool (e.g., DB Browser for SQLite).

## Future Ideas

- Expand to PostgreSQL
- Add dbt (data modeling)
- Deploy as a simple API with FastAPI or Flask

## About

reel-etl is a personal project focused on building clean, simple data engineering workflows and exploring movie data.
