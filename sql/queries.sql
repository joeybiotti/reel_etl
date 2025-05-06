-- Queries for REEL ETL Project
-- Dataset: Movies metadata
/* All movies titles + release year */
SELECT
    a.movie_title,
    a.title_year
FROM
    movies a
;

/* All movies with IMDb score > 8 */
SELECT
    a.movie_title,
    a.imdb_score
FROM
    movies a
WHERE
    a.imdb_score>8
ORDER BY
    a.imdb_score DESC
;

/* Count of number movies in dataset */
SELECT
    COUNT(*)
FROM
    movies
;

/* Top 5 countries by number of movies produced */
SELECT
    a.country,
    COUNT(*) AS movie_count
FROM
    movies a
GROUP BY
    a.country
ORDER BY
    movie_count DESC
LIMIT
    5
;

/* Directors who directed more thatn 6 movies */
SELECT
    a.director_name,
    COUNT(*) AS movie_count
FROM
    movies a
GROUP BY
    a.director_name
HAVING
    movie_count>5
ORDER BY
    movie_count DESC
;

/* Movies with highest gross revenue per minute */
SELECT
    a.movie_title,
    (a.gross/a.duration) AS gross_per_minute
FROM
    movies a
WHERE
    a.gross IS NOT NULL
    AND a.duration IS NOT NULL
ORDER BY
    gross_per_minute DESC
LIMIT
    5
;

/* Rank movies within country by IMDb score */
SELECT
    a.movie_title,
    a.country,
    a.imdb_score,
    RANK() OVER (
        PARTITION BY
            a.country
        ORDER BY
            a.imdb_score DESC
    ) AS rank_in_country
FROM
    movies a
WHERE
    a.country IS NOT NULL
ORDER BY
    a.country,
    rank_in_country
;

/* Running total of movies released each year */
SELECT
    a.title_year,
    COUNT(*),
    SUM(COUNT(*)) OVER (
        PARTITION BY
            title_year
    ) AS cume_movies
FROM
    movies a
WHERE
    a.title_year IS NOT NULL
GROUP BY
    a.title_year
ORDER BY
    a.title_year
;

/* Actor who appeared most frequently in highest movie gross */
WITH
    actor_movie_gross AS (
        SELECT
            actor_1_name AS actor_name,
            gross
        FROM
            movies
        WHERE
            actor_1_name IS NOT NULL
        UNION ALL
        SELECT
            actor_2_name AS actor_name,
            gross
        FROM
            movies
        WHERE
            actor_2_name IS NOT NULL
        UNION ALL
        SELECT
            actor_1_name AS actor_name,
            gross
        FROM
            movies
        WHERE
            actor_1_name IS NOT NULL
    ),
    actor_totals AS (
        SELECT
            actor_name,
            COUNT(*) AS appearances,
            SUM(gross) AS total_gross
        FROM
            actor_movie_gross
        WHERE
            gross IS NOT NULL
        GROUP BY
            actor_name
    )
SELECT
    actor_name,
    appearances,
    total_gross
FROM
    actor_totals
ORDER BY
    total_gross desc
LIMIT
    1
;