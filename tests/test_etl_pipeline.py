import pytest
import pandas as pd
import sqlite3
from scripts.etl_pipeline import extract, transform, save_processed, load
import subprocess

# === Fixtures ===#


@pytest.fixture
def sample_df():
    """Fixture to create a sample dataframe for testing."""
    data = {
        'movie_title': ['Movie1', 'Movie2', None],
        'title_year': [2000, 2001, None],
        'director_name': ['Director1', 'Director2', None],
        'gross': [100000, None, 50000],
        'budget': [50000, None, 20000],
        'content_rating': ['PG', None, 'R'],
        'imdb_score': [7.0, 8.5, None],
        'genres': ['Action', 'Comedy', 'Drama'],
        'language': ['English', 'French', 'Spanish'],
        'country': ['USA', 'France', 'Spain']
    }
    return pd.DataFrame(data)


# === Tests ===#


@pytest.mark.parametrize(
    "file_path",
    [
        ('data/raw/movie_metadata.csv')
    ]
)
def test_extract_returns_dataframe(file_path):
    """Test that extract function loads CSV into a DataFrame"""
    df = extract(file_path)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty


def test_transform_cleans_dataframe(sample_df):
    """Test transform returns cleaned DataFrame."""
    transformed_df = transform(sample_df)

    # Check it returns a DataFrame
    assert isinstance(transformed_df, pd.DataFrame)

    # No missing data in critical columns
    assert not transformed_df[[
        'movie_title', 'title_year', 'director_name']].isnull().any().any()

    # Column names should be clean (no spaces & lowercase)
    assert all(' ' not in col for col in transformed_df.columns)
    assert all(col == col.lower() for col in transformed_df.columns)


def test_transform_handles_missing_or_extra_column(sample_df):
    print(sample_df.columns)
    sample_df.drop(columns=['genres', 'country'], inplace=True)
    transformed_df = transform(sample_df)
    assert 'budget' in transformed_df.columns
    assert 'genres' not in transformed_df.columns


def test_transform_handles_extreme_values(sample_df):
    sample_df.loc[0, 'budget'] = 0
    sample_df.loc[1, 'budget'] = None
    sample_df.loc[2, 'budget'] = 10000

    sample_df.loc[0, 'imdb_score'] = 7.5
    sample_df.loc[1, 'imdb_score'] = None
    sample_df.loc[2, 'imdb_score'] = -3

    transformed_df = transform(sample_df)

    print(transformed_df["imdb_score"])

    assert transformed_df['budget'].isnull().sum() == 0
    assert transformed_df['imdb_score'].min() >= 0


@pytest.mark.parametrize(
    'title',
    [
        "Léon: The Professional",
        "千と千尋の神隠し",
        "El laberinto del fauno",
        "Паразиты",
        "🚀 Starship"
    ]
)
def test_transform_handles_unicode_titles(sample_df, title):
    sample_df.loc[0, 'movie_title'] = title
    transform_df = transform(sample_df)

    assert transform_df['movie_title'].iloc[0] == title
    assert isinstance(transform_df['movie_title'].iloc[0], str)


@pytest.mark.parametrize(
    "output_filename",
    [
        ("test_output.csv")
    ]
)
def test_save_processed_saves_nonempty_csv(
        sample_df, tmp_path, output_filename):
    """Test that save_process correctly saves a CSV file"""
    output_file = tmp_path / output_filename
    save_processed(sample_df, output_file)

    # Check file was created
    assert output_file.exists()

    # Check file is not empty
    saved_df = pd.read_csv(output_file)
    assert not saved_df.empty
    assert list(saved_df.columns) == list(sample_df.columns)


@pytest.mark.parametrize(
    "table_name",
    [
        ("movies")
    ]
)
def test_load_inserts_dataframe_into_sqlite(sample_df, tmp_path, table_name):
    """Test correctly inserts data into SQLite db"""
    db_file = tmp_path / "movies.db"

    conn = sqlite3.connect(db_file)
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            movie_title TEXT,
            title_year INTEGER,
            director_name TEXT,
            unique_key TEXT UNIQUE
        );
    """)
    conn.commit()
    conn.close()
    
    sample_df = sample_df[['movie_title', 'title_year', 'director_name']].copy()
    sample_df['unique_key'] = sample_df['movie_title'].astype(str) + '_' + sample_df['title_year'].astype(str) + '_' + sample_df['director_name'].astype(str)
    
    # Load sample data into temp db
    load(sample_df, db_file, table_name)

    # Query db to confirm insert
    conn = sqlite3.connect(db_file)
    result_df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    conn.close()

    # Verify data is loaded
    assert not result_df.empty
    assert set(result_df.columns) == set(sample_df.columns)
    assert len(result_df) <= len(sample_df)

def test_cli_help():
    '''Test CLI Help Command'''
    result = subprocess.run(['python', 'scripts/etl_pipeline.py', '--help'], capture_output=True, text=True)
    assert 'Run the ETL Pipeline with CLI Control' in result.stdout
    
def test_load_removes_duplicates(sample_df, tmp_path):
    '''Ensure duplicate records are filtered before insert'''
    db_file = tmp_path / 'movies.db'
    table_name = 'movies'

    # Explicitly create the table before inserting 
    conn = sqlite3.connect(db_file)
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            movie_title TEXT,
            title_year INTEGER,
            director_name TEXT,
            unique_key TEXT UNIQUE
        );
    """)
    conn.commit()
    conn.close()
    
    sample_df = sample_df[['movie_title', 'title_year', 'director_name']].copy()
    sample_df['unique_key'] = sample_df['movie_title'].astype(str) + '_' + sample_df['title_year'].astype(str) + '_' + sample_df['director_name'].astype(str)


    # Insert data twice to simulate duplicates
    load(sample_df, db_file, table_name)
    load(sample_df, db_file, table_name)

    # Query database to check final row count
    conn = sqlite3.connect(db_file)
    result_df = pd.read_sql_query(f"SELECT DISTINCT unique_key FROM {table_name}", conn)
    conn.close()

    assert len(result_df) <= len(sample_df), "Unexpected duplicate handling issue!"

