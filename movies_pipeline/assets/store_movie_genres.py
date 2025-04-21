import duckdb
import os
from dagster import asset
from filelock import FileLock

@asset
def store_movie_genres(context, fetch_movie_genres: list):
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    DB_PATH = os.path.abspath(os.path.join(BASE_DIR, "../../data/movies.duckdb"))
    LOCK_PATH = DB_PATH + ".lock"

    context.log.info(f"Connexion à la base : {DB_PATH}")

    with FileLock(LOCK_PATH):
        with duckdb.connect(DB_PATH, read_only=False) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS movie_genres (
                    id INTEGER PRIMARY KEY,
                    name TEXT
                )
            """)
            context.log.info("Table 'movie_genres' prête (créée si inexistante).")

            conn.execute("DELETE FROM movie_genres")
            context.log.info("Table 'movie_genres' vidée avant insertion.")

            inserted = 0
            for genre in fetch_movie_genres:
                try:
                    conn.execute(
                        "INSERT INTO movie_genres VALUES (?, ?)",
                        (genre["id"], genre["name"])
                    )
                    inserted += 1
                except Exception as e:
                    context.log.error(f"Erreur insertion genre ID {genre['id']} : {e}")

    context.log.info(f"{inserted} genres insérés dans la table 'movie_genres'.")
