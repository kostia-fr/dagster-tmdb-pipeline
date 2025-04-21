# movies_pipeline/schedules.py

from dagster import ScheduleDefinition
from movies_pipeline.jobs import daily_asset_job

daily_schedule = ScheduleDefinition(
    job=daily_asset_job,
    cron_schedule="0 6 * * *",  # Chaque jour à 6h
    name="daily_asset_schedule"
)
