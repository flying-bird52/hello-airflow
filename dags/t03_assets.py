"""Tutorial 06 - data-aware scheduling with Assets.

Two DAGs in one file:

* ``t03_asset_producer`` writes ``weather.json`` and declares it updates the ``weather`` asset.
* ``t03_asset_consumer`` has no time schedule: it runs **every time the asset is updated**.
"""

import json
import random
from pathlib import Path

import pendulum

from airflow.sdk import Asset, dag, task

WEATHER_FILE = Path("/opt/airflow/data/weather.json")
weather_asset = Asset(name="weather", uri=f"file://{WEATHER_FILE}")


@dag(
    dag_id="t03_asset_producer",
    description="Writes weather.json and updates the 'weather' asset",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["tutorial", "assets"],
)
def asset_producer():
    @task(outlets=[weather_asset])
    def fetch_weather():
        reading = {
            "city": "Paris",
            "temperature_c": round(random.uniform(-5, 35), 1),
            "measured_at": pendulum.now("UTC").to_iso8601_string(),
        }
        WEATHER_FILE.parent.mkdir(parents=True, exist_ok=True)
        WEATHER_FILE.write_text(json.dumps(reading, indent=2))
        print(f"Wrote {reading} to {WEATHER_FILE}")

    fetch_weather()


@dag(
    dag_id="t03_asset_consumer",
    description="Runs whenever the 'weather' asset is updated",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=[weather_asset],
    catchup=False,
    tags=["tutorial", "assets"],
)
def asset_consumer():
    @task
    def report():
        reading = json.loads(WEATHER_FILE.read_text())
        verdict = "take a coat" if reading["temperature_c"] < 15 else "t-shirt weather"
        print(f"{reading['city']}: {reading['temperature_c']} C -> {verdict}")

    report()


asset_producer()
asset_consumer()
