import os
import requests
from dagster import asset
from dotenv import load_dotenv

load_dotenv()

@asset
def fetch_tv_genres(context) -> list:
    token = os.getenv("TMDB_BEARER_TOKEN")
    url = "https://api.themoviedb.org/3/genre/tv/list?language=en"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {token}"
    }

    context.log.info("Fetching TV genres from TMDB API...")

    try:
        response = requests.get(url, headers=headers, verify=False)
        response.raise_for_status()
        genres = response.json().get("genres", [])
        context.log.info(f"{len(genres)} TV genres retrieved successfully.")
    except requests.RequestException as e:
        context.log.error(f"Error fetching TV genres from TMDB API: {e}")
        genres = []

    return genres

