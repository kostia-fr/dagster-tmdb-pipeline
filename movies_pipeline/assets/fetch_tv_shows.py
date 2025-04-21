import os
import requests
from dagster import asset
from dotenv import load_dotenv

load_dotenv()

@asset
def fetch_popular_tv_shows(context) -> list:
    token = os.getenv("TMDB_BEARER_TOKEN")
    base_url = "https://api.themoviedb.org/3/discover/tv"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {token}"
    }

    all_results = []
    page = 1
    total_pages = 1

    context.log.info("Fetching popular TV shows from TMDB...")

    try:
        while page <= total_pages and page <= 100:
            params = {
                "include_adult": "false",
                "include_null_first_air_dates": "false",
                "language": "en-US",
                "sort_by": "popularity.desc",
                "page": page
            }

            response = requests.get(base_url, headers=headers, params=params, verify=False)
            response.raise_for_status()
            data = response.json()

            if page == 1:
                total_pages = data.get("total_pages", 1)

            all_results.extend(data.get("results", []))
            context.log.info(f"Page {page}/{total_pages} récupérée")
            page += 1

    except requests.RequestException as e:
        context.log.error(f"Erreur lors de l'appel à l'API TMDB (TV shows): {e}")
        return []

    # Déduplication
    seen_ids = set()
    unique_tv_shows = []

    for show in all_results:
        show_id = show.get("id")
        if show_id and show_id not in seen_ids:
            seen_ids.add(show_id)
            unique_tv_shows.append(show)

    context.log.info(f"{len(all_results)} séries récupérées, {len(unique_tv_shows)} après déduplication.")
    return unique_tv_shows
