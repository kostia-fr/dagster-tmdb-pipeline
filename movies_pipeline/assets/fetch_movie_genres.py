import os
import requests
from dagster import asset
from dotenv import load_dotenv

load_dotenv()

@asset
def fetch_movie_genres(context) -> list:
    token = os.getenv("TMDB_BEARER_TOKEN")
    url = "https://api.themoviedb.org/3/genre/movie/list?language=en"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {token}"
    }

    context.log.info("Requête vers l'API TMDB pour récupérer les genres de films...")

    try:
        response = requests.get(url, headers=headers, verify=False)
        response.raise_for_status()
        data = response.json()
        genres = data.get("genres", [])
        context.log.info(f"{len(genres)} genres de films récupérés avec succès.")
    except requests.RequestException as e:
        context.log.error(f"Erreur lors de l'appel à l'API TMDB : {e}")
        genres = []

    return genres
