import duckdb
import os
import json
from dagster import asset
from filelock import FileLock

@asset
def store_tv_shows(context, fetch_popular_tv_shows: list):
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    DB_PATH = os.path.abspath(os.path.join(BASE_DIR, "../../data/movies.duckdb"))
    LOCK_PATH = DB_PATH + ".lock"

    context.log.info(f"Connexion à la base : {DB_PATH}")

    with FileLock(LOCK_PATH):
        with duckdb.connect(DB_PATH, read_only=False) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS popular_tv_shows (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    first_air_date DATE,
                    vote_average FLOAT,
                    popularity FLOAT,
                    overview TEXT,
                    vote_count INTEGER,
                    genre_ids TEXT
                )
            """)
            context.log.info("Table 'popular_tv_shows' prête (créée si inexistante).")

            inserted = 0
            for show in fetch_popular_tv_shows:
                show_id = show.get("id")
                name = show.get("name")
                date = show.get("first_air_date")
                vote = show.get("vote_average")
                popularity = show.get("popularity")
                overview = show.get("overview")
                vote_count = show.get("vote_count")
                genre_ids = json.dumps(show.get("genre_ids", []))

                if not date or date.strip() == "":
                    date = None

                try:
                    conn.execute(
                        """
                        INSERT INTO popular_tv_shows (
                            id, name, first_air_date, vote_average,
                            popularity, overview, vote_count, genre_ids
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (show_id, name, date, vote, popularity, overview, vote_count, genre_ids)
                    )
                    inserted += 1
                except Exception as e:
                    context.log.error(f"Erreur insertion série '{name}': {e}")

    context.log.info(f"{inserted} séries insérées dans la table 'popular_tv_shows'.")
