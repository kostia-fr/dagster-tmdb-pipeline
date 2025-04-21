# Pipeline de Données Dagster à partir de l'API TMDB

Ce projet met en œuvre une pipeline de données avec **Dagster** pour l'orchestration, **DuckDB** pour le stockage, et **Power BI** pour la visualisation, à partir de données issues de l'API **TMDB (The Movie Database)**.

---

## Choix de conception

### Source de données
- API TMDB (films, séries TV, genres)
- Appels paginés avec gestion des doublons
- Partitionnement par date (`primary_release_date`) pour scalabilité

### Orchestration
- Dagster pour la gestion des assets, jobs, partitions, schedules
- Pipeline décomposée en assets unitaires (`fetch_*`, `store_*`)
- Utilisation des `DailyPartitionsDefinition` pour rejouabilité

### Stockage
- Base **DuckDB locale**, légère et rapide
- Une table par entité : `popular_movies`, `popular_tv_shows`, `movie_genres`, `tv_genres`
- Tracabilité des exécutions via un champ `partition`

### Visualisation
- Connexion directe à DuckDB dans Power BI
- Dashboards interactifs : top films, tendances par jour, genres les plus populaires

---

## Installation & Déploiement

### 1. Prérequis

- Python 3.10+
- `pip`
- Compte TMDB + token d'authentification
- Power BI Desktop

### 2. Installation du projet

```bash
git clone https://github.com/votre-utilisateur/4DATA-Dagster.git
cd 4DATA-Dagster
python -m venv .venv
source .venv\Scripts\activate
pip install -r requirements.txt
