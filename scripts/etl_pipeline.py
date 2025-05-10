import pandas as pd
import sqlite3
import os
import logging
import argparse
import time
import functools
from logging.handlers import RotatingFileHandler
from pythonjsonlogger import jsonlogger
import tomli
import sys


# Debug mode configuration
DEBUG_MODE = os.getenv('DEBUG_MODE', 'False').lower() == 'true'

# Create logs directory
os.makedirs('logs', exist_ok=True)

# Set global logging configuration FIRST
logging.basicConfig(level=logging.DEBUG if DEBUG_MODE else logging.INFO)

# Set up logger AFTER configuring logging globally
logger = logging.getLogger('ETL_Pipeline')
logger.setLevel(logging.DEBUG if DEBUG_MODE else logging.INFO)

# Remove any existing handlers to prevent conflict
for handler in logger.handlers[:]:
    logger.removeHandler(handler)

# Console Handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter(
    '%(asctime)s [%(levelname)s] %(message)s'))
console_handler.setLevel(logging.DEBUG if DEBUG_MODE else logging.INFO)
logger.addHandler(console_handler)

# Filer handler
file_handler = logging.FileHandler('logs/etl_pipeline.log')
file_handler.setFormatter(logging.Formatter(
    '%(asctime)s [%(levelname)s] %(message)s'))
file_handler.setLevel(logging.DEBUG if DEBUG_MODE else logging.INFO)
logger.addHandler(file_handler)

# JSON handler
json_file_handler = logging.FileHandler('logs/etl_pipeline.json')
json_formatter = jsonlogger.JsonFormatter(
    '%(asctime)s %(levelname)s %(name)s %(message)s %(filename)s %(funcName)s')
json_file_handler.setFormatter(json_formatter)
json_file_handler.setLevel(logging.DEBUG if DEBUG_MODE else logging.INFO)
logger.addHandler(json_file_handler)


def load_config(path='config.toml'):
    with open(path, 'rb') as f:
        return tomli.load(f)


config = load_config()


def log_time(func):
    """Decorator to log the execution time of a function."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        logger.info(f'Running function: {func.__name__}')
        result = func(*args, **kwargs)
        elapsed_time = time.time() - start_time
        logger.info(
            f"{func.__name__} completed in {elapsed_time:.2f} seconds.")
        return result
    return wrapper


@log_time
def extract(file_path):
    """Extract data from CSV."""
    logger.info("Extracting data...")
    logger.debug(
        f"Extracting {file_path}, file exists: {os.path.exists(file_path)}")
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    df = pd.read_csv(file_path)
    return df


@log_time
def transform(df):
    """Apply all data cleaning steps."""
    logger.info("Transforming data...")
    df = df.copy()
    df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
    df = df.dropna(subset=['movie_title', 'title_year', 'director_name'])
    df['budget'] = df['budget'].fillna(0).astype(float)
    df['gross'] = df['gross'].fillna(0).astype(float)
    df['imdb_score'] = df['imdb_score'].fillna(0).astype(float)
    return df


@log_time
def save_processed(df, output_path):
    """Save the cleaned data frame."""
    logger.info("Saving cleaned data...")
    logger.debug(f"Saving clearned data to {output_path}")
    df.to_csv(output_path, index=False)


@log_time
def load(df, db_path, table_name):
    """Load data into SQLite database."""
    logger.info("Loading cleaned data into database...")
    logger.debug(
        f"Loading data into database: {db_path} , table name: {table_name}")
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Run the ETL Pipeline for movie metadata.")
    parser.add_argument('--raw_data', type=str, default=config['paths']['raw_data_file'],
                        help=f'Path to raw CSV file (Default: {config["paths"]["raw_data_file"]})')
    parser.add_argument('--processed_data', type=str, default=config['paths']['processed_data_file'],
                        help=f'Path to processed/cleaned CSV file (Default: {config["paths"]["processed_data_file"]})')
    parser.add_argument('--db_path', type=str, default=config['paths'].get(
        'db_path', 'movies.db'), help='Path to SQLite database')

    parser.add_argument('--table_name', type=str, default=config['paths']['table_name'],
                        help=f'Table name in the database (Default: {config["paths"]["table_name"]})')
    parser.add_argument('--debug', action='store_true',
                        help='Enable debug mode')

    args = parser.parse_args()

    return args


def main():
    """Run the ETL pipeline."""
    args = parse_args()

    DEBUG_MODE = args.debug
    logger.setLevel(logging.DEBUG if DEBUG_MODE else logging.INFO)

    # Override handler levels AFTER parsing arguments
    for handler in logger.handlers:
        handler.setLevel(logging.DEBUG if DEBUG_MODE else logging.INFO)

    try:
        df = extract(args.raw_data)
        df = transform(df)
        save_processed(df, args.processed_data)
        load(df, args.db_path, args.table_name)
        logger.info("ETL Pipeline completed successfully.")
    except Exception as e:
        logger.error(f"ETL Pipeline failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
