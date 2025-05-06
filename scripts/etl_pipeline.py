import pandas as pd
import sqlite3
import os 

RAW_DATA_FILE = 'data/raw/movie_metadata.csv'
DB_PATH ='movies.db'
TABLE_NAME='movies'

# Extract
def extract(file_path):
    """Extract data from CSV."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found : {file_path}")
    df = pd.read_csv(file_path)
    return df

# Transform 
def transform(df):
    """Basic transformation: clean column names."""
    df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
    return df

# Load
def load(df, db_path, table_name):
    """Load data into SQLite db"""
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name,conn, if_exists='replace', index=False)
    conn.close()

def main():
    """Run the ELT pipeline"""
    try:
        df = extract(RAW_DATA_FILE)
        df= transform(df)
        load(df, DB_PATH, TABLE_NAME)
        print("ETL Pipeline completed successfully.")
    except Exception as e:
        print(f"ELT Pipeline failed: {e}")
        
if __name__ == '__main__':
    main()