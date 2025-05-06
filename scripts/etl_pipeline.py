import pandas as pd
import sqlite3

# Extract
print("Extracting data...")
df = pd.read_csv('data/raw/movie_metadata.csv')

# Transform 
print("Transforming data...")
df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
df = df.dropna()

# Load
print("Loading data into SQLite...")
conn = sqlite3.connect('movies.db')
df.to_sql('movies', conn,if_exists='replace', index=False)
print(pd.read_sql_query("SELECT * FROM movies LIMIT 5;",conn))
conn.close()

print("ETL Process complete.")