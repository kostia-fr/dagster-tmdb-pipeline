import duckdb
import os
import json
from dagster import asset, DailyPartitionsDefinition
from filelock import FileLock

partitions_def = DailyPartitionsDefinition(start_date="2024-01-01")

@asset(partitions_def=partitions_def)
def store_movies(context, fetch_popular_movies: list):
    partition_date = context.partition_key

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    DB_PATH = os.path.abspath(os.path.join(BASE_DIR, "../../data/movies.duckdb"))
    LOCK_PATH = DB_PATH + ".lock"

    context.log.info(f"Connexion à la base : {DB_PATH}")

    with FileLock(LOCK_PATH):
        with duckdb.connect(DB_PATH, read_only=False) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS popular_movies (
                    id INTEGER,
                    title TEXT,
                    release_date DATE,
                    vote_average FLOAT,
                    popularity FLOAT,
                    overview TEXT,
                    vote_count INTEGER,
                    genre_ids TEXT,
                    partition TEXT
                )
            """)
            context.log.info("Table 'popular_movies' prête (créée si inexistante).")

            inserted = 0
            for movie in fetch_popular_movies:
                movie_id = movie.get("id")
                title = movie.get("title")
                date = movie.get("release_date")
                vote = movie.get("vote_average")
                popularity = movie.get("popularity")
                overview = movie.get("overview")
                vote_count = movie.get("vote_count")
                genre_ids = json.dumps(movie.get("genre_ids", []))

                if not date or date.strip() == "":
                    date = None

                try:
                    conn.execute(
                        """
                        INSERT INTO popular_movies (
                            id, title, release_date, vote_average,
                            popularity, overview, vote_count, genre_ids, partition
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (movie_id, title, date, vote, popularity, overview, vote_count, genre_ids, partition_date)
                    )
                    inserted += 1
                except Exception as e:
                    context.log.error(f"Erreur insertion film '{title}': {e}")

    context.log.info(f"{inserted} films insérés dans 'popular_movies' pour la partition {partition_date}.")
