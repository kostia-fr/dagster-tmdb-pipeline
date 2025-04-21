from dagster import Definitions

# Assets : films
from movies_pipeline.assets.fetch_movies import fetch_popular_movies
from movies_pipeline.assets.store_movies import store_movies

# Assets : séries
from movies_pipeline.assets.fetch_tv_shows import fetch_popular_tv_shows
from movies_pipeline.assets.store_tv_shows import store_tv_shows

# Assets : genres
from movies_pipeline.assets.fetch_tv_genres import fetch_tv_genres
from movies_pipeline.assets.store_tv_genres import store_tv_genres
from movies_pipeline.assets.fetch_movie_genres import fetch_movie_genres
from movies_pipeline.assets.store_movie_genres import store_movie_genres

# Jobs / Schedules / Sensors
from movies_pipeline.jobs import daily_asset_job
from movies_pipeline.schedules import daily_schedule
from movies_pipeline.sensors import new_movie_sensor

defs = Definitions(
    assets=[
        fetch_popular_movies,
        store_movies,
        fetch_popular_tv_shows,
        store_tv_shows,
        fetch_tv_genres,
        store_tv_genres,
        fetch_movie_genres,
        store_movie_genres
    ],
    jobs=[daily_asset_job],
    schedules=[daily_schedule],
    sensors=[new_movie_sensor],
)
