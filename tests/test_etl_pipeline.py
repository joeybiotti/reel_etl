import pytest
import pandas as pd
import sqlite3
from scripts.etl_pipeline import extract, transform, save_processed, load

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
    sample_df.drop(columns=['genres','country'], inplace=True)
    transformed_df = transform(sample_df)
    assert 'budget' in transformed_df.columns
    assert 'genres' not in transformed_df.columns

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
        ("test_movies")
    ]
)
def test_load_inserts_dataframe_into_sqlite(sample_df, tmp_path, table_name):
    """Test correctly inserts data into SQLite db"""
    db_file = tmp_path / "test_movies.db"

    # Load sample data into temp db
    load(sample_df, db_file, table_name)

    # Query db to confirm insert
    conn = sqlite3.connect(db_file)
    result_df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    conn.close()

    # Verify data is loaded
    assert not result_df.empty
    assert set(result_df.columns) == set(sample_df.columns)
    assert len(result_df) == len(sample_df)
