"""Tutorial 04 & 05 - a small ETL pipeline written with the TaskFlow API.

extract  ->  transform  ->  load

* Runs every day (``@daily``) without backfilling the past (``catchup=False``).
* Values returned by a task are passed to the next one through **XCom**.
* ``load`` writes a CSV into ``/opt/airflow/data`` which is ``./data/files`` on your machine.
"""

import csv
import random
from pathlib import Path

import pendulum

from airflow.sdk import dag, task

OUTPUT_DIR = Path("/opt/airflow/data/sales")


@dag(
    dag_id="t02_taskflow_etl",
    description="Extract, transform, load a tiny fake sales dataset",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["tutorial", "taskflow"],
    doc_md=__doc__,
)
def taskflow_etl():
    @task
    def extract() -> list[dict]:
        """Pretend to call an API and get some orders back."""
        products = ["coffee", "tea", "cake", "juice"]
        orders = [
            {"order_id": i, "product": random.choice(products), "amount": round(random.uniform(2, 20), 2)}
            for i in range(1, 21)
        ]
        print(f"Extracted {len(orders)} orders")
        return orders

    @task
    def transform(orders: list[dict]) -> dict[str, float]:
        """Total revenue per product."""
        totals: dict[str, float] = {}
        for order in orders:
            totals[order["product"]] = round(totals.get(order["product"], 0) + order["amount"], 2)
        print(f"Totals: {totals}")
        return totals

    @task
    def load(totals: dict[str, float], ds=None) -> str:
        """Write one CSV per run. ``ds`` is injected by Airflow: the run's date as YYYY-MM-DD."""
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        path = OUTPUT_DIR / f"sales_{ds}.csv"
        with path.open("w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["product", "revenue"])
            writer.writerows(sorted(totals.items()))
        print(f"Wrote {path}")
        return str(path)

    load(transform(extract()))


taskflow_etl()
