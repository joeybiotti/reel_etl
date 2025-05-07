import pandas as pd
import sqlite3
import os
import logging
import argparse

from logging.handlers import RotatingFileHandler

os.makedirs('logs', exist_ok=True)

# Configure logging
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

# File paths and constants
RAW_DATA_FILE = 'data/raw/movie_metadata.csv'
PROCESSED_DATA_FILE = 'data/processed/movie_metadata_cleaned.csv'
DB_PATH = 'movies.db'
TABLE_NAME = 'movies'

# Extract
def extract(file_path):
    """Extract data from CSV."""
    logging.info("Extracting data...")
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    df = pd.read_csv(file_path)
    return df

# Transform helper functions
def clean_column_names(df):
    """Clean column names by stripping spaces, converting to lowercase, and replacing spaces with underscores."""
    logging.info("Cleaning column names...")
    df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
    return df

def drop_missing_critical(df):
    """Drop rows with missing values in critical columns."""
    logging.info("Dropping rows with missing critical columns...")
    critical_columns = ['movie_title', 'title_year', 'director_name']
    df = df.dropna(subset=critical_columns)
    return df

def fill_missing_values(df):
    """Fill missing values in specific columns with default values."""
    logging.info("Filling missing values in specific columns with default values...")
    df.loc[:, 'gross'] = df['gross'].fillna(0)
    df.loc[:, 'budget'] = df['budget'].fillna(0)
    df.loc[:, 'content_rating'] = df['content_rating'].fillna('Not Rated')
    return df

def fix_data_types(df):
    """Fix data types for specific columns."""
    logging.info("Fixing data types for 'title_year', 'budget', 'gross', 'imdb_score'")
    df.loc[:, 'title_year'] = df['title_year'].astype('Int64')
    df.loc[:, 'budget'] = df['budget'].fillna(0).astype(float)
    df.loc[:, 'gross'] = df['gross'].fillna(0).astype(float)
    df.loc[:, 'imdb_score'] = df['imdb_score'].fillna(0).astype(float)
    return df

def normalize_text_fields(df):
    """Normalize text fields by converting to lowercase."""
    logging.info("Normalizing text fields text fields: 'genres', 'language', 'country'...")
    for col in ['genres', 'language', 'country']:
        if col in df.columns:
            df.loc[:, col] = df[col].astype(str).str.lower()
    return df

# Full Transform
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
def save_processed(df, output_path):
    """Save the cleaned data frame to a processed data file."""
    logging.info("Saving cleaned data...")
    df.to_csv(output_path, index=False)

# Load
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