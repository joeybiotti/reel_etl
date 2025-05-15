DROP TABLE IF EXISTS movies
;

CREATE TABLE
    movies (
        id INTEGER PRIMARY key AUTOINCREMENT,
        color TEXT,
        director_name TEXT,
        num_critic_for_reviews INTEGER,
        duration REAL,
        director_facebook_likes INTEGER,
        actor_3_facebook_likes INTEGER,
        actor_2_name TEXT,
        actor_1_facebook_likes INTEGER,
        gross REAL,
        genres TEXT,
        actor_1_name TEXT,
        movie_title TEXT,
        num_voted_users INTEGER,
        cast_total_facebook_likes INTEGER,
        actor_3_name TEXT,
        facenumber_in_poster INTEGER,
        plot_keywords TEXT,
        movie_imdb_link TEXT,
        num_user_for_reviews INTEGER,
        LANGUAGE TEXT,
        country TEXT,
        content_rating TEXT,
        budget REAL,
        title_year INTEGER,
        actor_2_facebook_likes INTEGER,
        imdb_score REAL,
        aspect_ratio REAL,
        movie_facebook_likes INTEGER,
        unique_key text UNIQUE
    )
;

CREATE INDEX idx_genres ON movies (genres)
;

CREATE INDEX idx_country ON movies (country)
;

CREATE INDEX idx_title_year ON movies (title_year)
;

CREATE UNIQUE INDEX unique_idx ON movies (unique_key)
;