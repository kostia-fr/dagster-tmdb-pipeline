from dagster import sensor, RunRequest
import requests
import os
from movies_pipeline.jobs import daily_asset_job
from dotenv import load_dotenv

load_dotenv()

LAST_RUN_FILE = "last_movie_check.txt"

def get_latest_movie_id():
    url = "https://api.themoviedb.org/3/movie/popular"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiI3ZjEwYzU2ZWM4MmVmY2YyMzllOWY2Y2ZmN2E1OTk0MSIsIm5iZiI6MTc0NDgwMzQyNC45NzMsInN1YiI6IjY3ZmY5NjYwZDY0NWU0MWUwOTk5NTc1MSIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.CAhXiZSPgGFHByX8uLofEPYontE8_t6mGMIv81SFW_E"
    }

    response = requests.get(url, headers=headers, verify=False)
    if response.status_code != 200:
        return None

    data = response.json()
    results = data.get("results", [])
    if not results:
        return None

    return results[0]["id"]  # Le film le plus récent/populaire

@sensor(job=daily_asset_job, minimum_interval_seconds=60)
def new_movie_sensor(context):
    latest_id = get_latest_movie_id()

    if latest_id is None:
        context.log.info("Aucun film disponible.")
        return

    # Lire la dernière ID stockée
    if os.path.exists(LAST_RUN_FILE):
        with open(LAST_RUN_FILE, "r") as f:
            last_id = f.read().strip()
    else:
        last_id = None

    # Si nouveau film détecté
    if str(latest_id) != last_id:
        context.log.info(f"Nouveau film détecté : {latest_id}")
        # On stocke la nouvelle ID
        with open(LAST_RUN_FILE, "w") as f:
            f.write(str(latest_id))
        return RunRequest(run_key=str(latest_id), run_config={})
    
    context.log.info("Aucune nouveauté détectée.")
    return None