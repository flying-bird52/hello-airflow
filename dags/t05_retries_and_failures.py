"""Tutorial 08 - what failure looks like, and how retries help.

``flaky_api_call`` fails about half the time. It is retried up to 3 times, 10 seconds apart.
``always_fails`` fails on purpose so you can practise reading logs and clearing tasks.
``cleanup`` uses ``trigger_rule="all_done"``: it runs whatever happened upstream.
"""

import random
from datetime import timedelta

import pendulum

from airflow.sdk import dag, task


@dag(
    dag_id="t05_retries_and_failures",
    description="Retries, failures and trigger rules",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["tutorial", "debugging"],
    default_args={"retries": 3, "retry_delay": timedelta(seconds=10)},
    doc_md=__doc__,
)
def retries_and_failures():
    @task
    def flaky_api_call():
        if random.random() < 0.5:
            raise ConnectionError("The (pretend) API timed out - Airflow will retry me.")
        print("API answered, all good.")

    @task(retries=0)
    def always_fails():
        raise ValueError("I fail on purpose. Read this message in the task logs!")

    @task(trigger_rule="all_done")
    def cleanup():
        print("Cleaning up, whatever happened before me.")

    [flaky_api_call(), always_fails()] >> cleanup()


retries_and_failures()
