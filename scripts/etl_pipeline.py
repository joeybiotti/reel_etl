import pandas as pd
import sqlite3
import os
import logging
import argparse
from time import perf_counter
import functools
from logging.handlers import RotatingFileHandler
from pythonjsonlogger import jsonlogger
import tomli
import sys
import psutil
from tqdm import tqdm
import typer

app = typer.Typer()

# Debug mode configuration
DEBUG_MODE = os.getenv('DEBUG_MODE', 'False').lower() == 'true'

# Create logs directory
os.makedirs('logs', exist_ok=True)


LOG_LEVEL = logging.DEBUG if DEBUG_MODE else logging.INFO
logging.basicConfig(level=LOG_LEVEL)

# Set up logger AFTER configuring logging globally
logger = logging.getLogger('ETL_Pipeline')
logger.setLevel(LOG_LEVEL)

# Remove any existing handlers to prevent conflict
for handler in logger.handlers[:]:
    logger.removeHandler(handler)

# Console Handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter(
    '%(asctime)s [%(levelname)s] %(message)s',
    "%Y-%m-%dT%H:%M:%S"))
console_handler.setLevel(LOG_LEVEL)
logger.addHandler(console_handler)

# File handler
file_handler = logging.FileHandler('logs/etl_pipeline.log')
file_handler.setFormatter(logging.Formatter(
    '%(asctime)s [%(levelname)s] %(message)s',
    "%Y-%m-%dT%H:%M:%S"))
file_handler.setLevel(LOG_LEVEL)
logger.addHandler(file_handler)

# JSON handler
json_file_handler = logging.FileHandler('logs/etl_pipeline.json')
json_formatter = jsonlogger.JsonFormatter(
    '%(asctime)s %(levelname)s %(name)s %(message)s %(filename)s %(funcName)s %(extra)',
    json_ensure_ascii=False
)
json_formatter.datefmt = "%Y-%m-%dT%H:%M:%S"
json_file_handler.setFormatter(json_formatter)
json_file_handler.setLevel(LOG_LEVEL)
logger.addHandler(json_file_handler)


def load_config(path='config.toml'):
    with open(path, 'rb') as f:
        return tomli.load(f)


config = load_config()


def log_time(func):
    """Decorator to log the execution time of a function."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = perf_counter()
        logger.info(f'Running function: {func.__name__}')
        result = func(*args, **kwargs)
        elapsed_time = perf_counter() - start_time
        logger.info(
            f"{func.__name__} completed in {elapsed_time:.2f} seconds.")
        return result
    return wrapper


def log_memory_usage():
    '''Log current memory usage'''
    memory_percent = psutil.virtual_memory().percent
    logger.info(f'Memory usage: {memory_percent:.2f}%')


def validate_file(path, description):
    '''Check if file exists and potential reason for failure.'''
    if not os.path.exists(path):
        reason = 'File does not exist'
        if not os.path.abspath(path).startswith(os.getcwd()):
            reason = "Path might be incorrect or outside expected directories"
        elif not os.access(path, os.R_OK):
            reason = "Insufficient permissions to read the file"

        raise FileNotFoundError(
            f"Missing {description}: {path}. Reason: {reason}")


@log_time
def extract(file_path):
    """Extract data from CSV."""
    logger.info("Extracting data...")
    log_memory_usage()
    logger.debug(
        f"Extracting {file_path}, file exists: {os.path.exists(file_path)}")

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    chunk_size = 10_000
    df_chunks = pd.read_csv(file_path, chunksize=chunk_size)

    df = pd.concat(tqdm(df_chunks, desc='Reading CSV chunks'),
                   ignore_index=True)
    return df


@log_time
def transform(df):
    """Apply all data cleaning steps."""
    logger.info("Transforming data...")
    log_memory_usage()

    tqdm.pandas(desc='Applying cleaning steps')
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
    log_memory_usage()
    logger.debug(f"Saving clearned data to {output_path}")
    df.to_csv(output_path, index=False)


@log_time
def load(df, db_path, table_name):
    """Load data into SQLite database with a progress bar."""
    logger.info("Loading cleaned data into database...")
    log_memory_usage()

    with sqlite3.connect(db_path) as conn:
        start_time = perf_counter()
        conn.execute('PRAGMA optimize;')

        df['unique_key'] = df['movie_title'].astype(
            str) + '_' + df['title_year'].astype(str) + '_' + df['director_name'].astype(str)

        existing_keys = pd.read_sql(f'SELECT DISTINCT unique_key FROM {table_name}', conn)[
            'unique_key'].tolist()
        df = df[~df['unique_key'].isin(existing_keys)]  # Remove duplicates

        if not df.empty:
            query = f'''
            INSERT OR IGNORE INTO {table_name} (movie_title, title_year, director_name, unique_key) 
            VALUES (?, ?, ?, ?)
            '''
        for row in tqdm(df.itertuples(index=False, name=None), desc="Inserting rows"):
            conn.execute(query, tuple(row))

            logger.info(f'Inserted {len(df)} new records.')
        else:
            logger.info('No new records to insert.')

        logger.debug(
            f"Unique keys about to be inserted: {df['unique_key'].tolist()}")

        logger.info(
            f'Bulk insert completed in {perf_counter()-start_time:.2f} seconds.')


@app.command()
def run_etl(
    raw_data: str = typer.Option(
        config['paths']['raw_data_file'], help="Path to raw CSV"),
    processed_data: str = typer.Option(
        config['paths']['processed_data_file'], help='Path to the cleaned CSV'),
    db_path: str = typer.Option(config['paths'].get(
        'db_path', 'movies.db'), help='Path to SQLite database'),
    table_name: str = typer.Option(
        config['paths']['table_name'], help='Target database table'),
    debug: bool = typer.Option(False, '--debug', help='Enable debug mode'),
    extract_only: bool = typer.Option(
        False, '--extract-only', help='Enable extract only'),
    transform_only: bool = typer.Option(
        False, '--transform_only', help='Enable transform only'),
    load_only: bool = typer.Option(
        False, '--load-only', help='Enable load only')
):
    '''Run the ETL Pipeline with CLI Control'''
    if debug:
        logger.setLevel(logging.DEBUG)
        logger.info('Debub mode enabled')

    if extract_only:
        df = extract(raw_data)
        return

    df = extract(raw_data)

    if transform_only:
        df = transform(df)
        save_processed(df, processed_data)
        return

    df = transform(df)
    save_processed(df, processed_data)

    if load_only:
        load(df, db_path, table_name)
        return

    load(df, db_path, table_name)
    logger.info('ETL Pipeline completed successfully!')


if __name__ == '__main__':
    app()
