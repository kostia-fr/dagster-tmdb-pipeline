import os
import requests
from dagster import asset, DailyPartitionsDefinition, Output
from dotenv import load_dotenv

load_dotenv()

partitions_def = DailyPartitionsDefinition(start_date="2024-01-01")

@asset(partitions_def=partitions_def)
def fetch_popular_movies(context) -> Output[list]:
    partition_date = context.partition_key

    token = os.getenv("TMDB_BEARER_TOKEN")
    base_url = "https://api.themoviedb.org/3/discover/movie"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {token}"
    }

    context.log.info(f"Fetching popular movies for partition date: {partition_date}")

    all_results = []
    page = 1
    total_pages = 1

    try:
        while page <= total_pages and page <= 100:
            params = {
                "include_adult": "false",
                "include_video": "false",
                "language": "en-US",
                "sort_by": "popularity.desc",
                "primary_release_date.gte": partition_date,
                "primary_release_date.lte": partition_date,
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
        context.log.error(f"Erreur lors de l'appel à l'API TMDB: {e}")
        return Output([])

    seen_ids = set()
    unique_movies = []

    for movie in all_results:
        movie_id = movie.get("id")
        if movie_id and movie_id not in seen_ids:
            seen_ids.add(movie_id)
            unique_movies.append(movie)

    context.log.info(f"{len(all_results)} films récupérés, {len(unique_movies)} après déduplication.")
    return Output(unique_movies)
