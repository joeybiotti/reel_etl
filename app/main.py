from fastapi import FastAPI, HTTPException
import sqlite3
import pandas as pd

app = FastAPI()

DB_PATH = 'movies.db'


def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


@app.get("/")
def read_root():
    return {"message": "Welcome to the Movies API"}


@app.get("/movies")
def get_movies(limit: int = 15):
    conn = get_db_connection()
    df = pd.read_sql_query(f"SELECT * FROM movies LIMIT {limit}", conn)
    conn.close()
    return df.to_dict(orient="records")


@app.get("/movies/{title}")
def get_movie_by_title(title: str):
    conn = get_db_connection()
    df = pd.read_sql_query(
        "SELECT * FROM movies WHERE lower(movie_title) like ?",
        conn,
        params=(f"%{title.lower()}%",)
    )
    conn.close()

    if df.empty:
        raise HTTPException(status_code=404, detail="Movie not found.")
    return df.to_dict(orient="records")[0]
