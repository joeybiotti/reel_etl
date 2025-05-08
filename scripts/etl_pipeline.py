import pandas as pd
import sqlite3
import os
import logging
import argparse
import time 
import functools
from logging.handlers import RotatingFileHandler
from pythonjsonlogger import jsonlogger

# Make /logs directory 
os.makedirs('logs', exist_ok=True)

# Console handler 
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter(
    "%(asctime)s [%(levelname)s] %(message)s"
)

console_handler.setFormatter(console_formatter)

# Create rotating file handler (JSON)
file_handler = RotatingFileHandler(
    'logs/etl_pipeline.json',
    maxBytes=5_000_000,
    backupCount=5,
    encoding='utf-8'
)
file_handler.setLevel(logging.INFO)
json_formatter = jsonlogger.JsonFormatter(
    '%(asctime)s %(levelname)s %(name)s %(message)s',
)
file_handler.setFormatter(json_formatter)

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(console_handler)
logging.getLogger().addHandler(file_handler)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        RotatingFileHandler(
            'logs/etl_pipeline.log',
            maxBytes=5_000_000,
            backupCount=5,
            encoding='utf-8'
        )
    ]
)

def log_time(func):
    """Decorator to log the time a function takes to execute."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        elapsed = time.time() - start_time
        logging.info(f"{func.__name__} completed in {elapsed:.2f} seconds.")
        return result
    return wrapper
    
# File paths and constants
RAW_DATA_FILE = 'data/raw/movie_metadata.csv'
PROCESSED_DATA_FILE = 'data/processed/movie_metadata_cleaned.csv'
DB_PATH = 'movies.db'
TABLE_NAME = 'movies'

# Extract
@log_time
def extract(file_path):
    """Extract data from CSV."""
    logging.info("Extracting data...")
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    df = pd.read_csv(file_path)
    return df

# Transform helper functions
@log_time
def clean_column_names(df):
    """Clean column names by stripping spaces, converting to lowercase, and replacing spaces with underscores."""
    logging.info("Cleaning column names...")
    df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
    return df

@log_time
def drop_missing_critical(df):
    """Drop rows with missing values in critical columns."""
    critical_columns = ['movie_title', 'title_year', 'director_name']
    before_rows = len(df)
    df = df.dropna(subset=critical_columns)
    after_rows = len(df)
    dropped = before_rows - after_rows
    logging.info(f'Dropped {dropped} rows missing critical columns.')
    return df

@log_time
def fill_missing_values(df):
    """Fill missing values in specific columns with default values."""
    for col, default in [('gross',0), ('budget',0), ('content_rating', 'Not Rated')]:
        missing_before = df[col].isnull().sum()
        df.loc[:, col] = df[col].fillna(default)
        missing_after =  df[col].isnull().sum()
        filled = missing_before - missing_after
        logging.info(f"Filled {filled} missing values in column '{col}'.")
    return df

@log_time
def fix_data_types(df):
    """Fix data types for specific columns."""
    logging.info("Fixing data types for 'title_year', 'budget', 'gross', 'imdb_score'")
    df.loc[:, 'title_year'] = df['title_year'].astype('Int64')
    df.loc[:, 'budget'] = df['budget'].fillna(0).astype(float)
    df.loc[:, 'gross'] = df['gross'].fillna(0).astype(float)
    df.loc[:, 'imdb_score'] = df['imdb_score'].fillna(0).astype(float)
    return df

@log_time
def normalize_text_fields(df):
    """Normalize text fields by converting to lowercase."""
    for col in ['genres', 'language', 'country']:
        if col in df.columns:
            logging.info(f"Normalizing text field '{col}'...")
            df.loc[:, col] = df[col].astype(str).str.lower()
    return df

# Full Transform
@log_time
def transform(df):
    """Apply all data cleaning steps."""
    logging.info("Transforming data...")
    df = clean_column_names(df)
    df = drop_missing_critical(df)
    df = fill_missing_values(df)
    df = fix_data_types(df)
    df = normalize_text_fields(df)
    return df

# Save processed data
@log_time
def save_processed(df, output_path):
    """Save the cleaned data frame to a processed data file."""
    logging.info("Saving cleaned data...")
    df.to_csv(output_path, index=False)

# Load
@log_time
def load(df, db_path, table_name):
    """Load data into SQLite database."""
    logging.info("Loading cleaned data into database...")
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()

def parse_args():
    parser = argparse.ArgumentParser(description="Run the ETL Pipeline for movie metadata.")
    parser.add_argument('--raw_data', type=str, default=RAW_DATA_FILE, help='Path to raw CSV file')
    parser.add_argument('--processed_data', type=str, default=PROCESSED_DATA_FILE, help='Path to processed/cleaned CSV file') 
    parser.add_argument('--db_path', type=str, default=DB_PATH, help='Path to SQLite database') 
    parser.add_argument('--table_name', type=str, default=TABLE_NAME, help='Table name in the database') 
    return parser.parse_args()

# Main ETL pipeline
def main():
    """Run the ETL pipeline."""
    args = parse_args()
    try:
        df = extract(args.raw_data)
        df = transform(df)
        save_processed(df, args.processed_data)
        load(df, args.db_path, args.table_name)
        logging.info("ETL Pipeline completed successfully.")
    except Exception as e:
        logging.error(f"ETL Pipeline failed: {e}")

if __name__ == '__main__':
    main()