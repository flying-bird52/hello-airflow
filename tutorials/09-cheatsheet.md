# 09 · Cheat sheet & next steps

## Docker Compose

| Command                                         | What it does                                  |
| ----------------------------------------------- | --------------------------------------------- |
| `docker compose up -d`                          | Start Airflow in the background               |
| `docker compose ps`                             | Status of every service                       |
| `docker compose logs -f airflow-scheduler`      | Follow a service's logs                       |
| `docker compose restart airflow-scheduler`      | Restart one service                           |
| `docker compose down`                           | Stop everything (keeps `./data`)              |
| `docker compose down` then `rm -rf data/postgres data/logs data/config data/files` | Full reset |
| `docker compose run --rm airflow-cli <cmd>`     | Run any `airflow <cmd>`                        |

## Airflow CLI (prefix with `docker compose run --rm airflow-cli`)

| Command                                             | What it does                          |
| --------------------------------------------------- | ------------------------------------- |
| `dags list`                                         | All DAGs                              |
| `dags list-import-errors`                           | Why a DAG doesn't show up             |
| `dags trigger <dag_id> -c '{"key": "value"}'`       | Trigger with params                   |
| `dags pause <dag_id>` / `dags unpause <dag_id>`     | Pause / unpause                       |
| `dags test <dag_id>`                                | Run a DAG once, logs in your terminal |
| `tasks test <dag_id> <task_id> <date>`              | Run a single task                     |
| `variables set <key> <value>` / `variables get <key>` | Manage variables                    |
| `connections list`                                  | List connections                      |
| `config get-value core executor`                    | Read a config value                   |

## DAG skeleton to copy-paste

```python
import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="my_pipeline",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 2},
    tags=["my-team"],
)
def my_pipeline():
    @task
    def extract() -> str:
        return "/opt/airflow/data/raw.json"

    @task
    def process(path: str, ds=None):
        print(f"processing {path} for {ds}")

    process(extract())


my_pipeline()
```

## Common pitfalls

| Symptom                                   | Likely cause                                                         |
| ----------------------------------------- | -------------------------------------------------------------------- |
| DAG doesn't appear in the UI              | Import error, duplicate `dag_id`, forgot to call `my_dag()`, or wait 30 s |
| DAG never runs                            | It's paused, or `schedule=None`                                      |
| Hundreds of runs appeared                 | `catchup=True` with an old `start_date`                              |
| Task stuck in *queued*                    | Scheduler down — check health badges on Home                         |
| `ModuleNotFoundError` in a task           | Package missing in the image: try `_PIP_ADDITIONAL_REQUIREMENTS` in `.env`, then build a custom image |
| UI slow / DB busy                         | Heavy code at the top level of a DAG file                            |

## Adding Python packages

Quick test: put `_PIP_ADDITIONAL_REQUIREMENTS=pandas requests` in `.env` and
`docker compose up -d`. It reinstalls on every start, so for real use build your own image:

```dockerfile
# Dockerfile
FROM apache/airflow:3.3.2
RUN pip install --no-cache-dir pandas
```

then set `AIRFLOW_IMAGE_NAME=my-airflow:latest` in `.env` after `docker build -t my-airflow:latest .`.

## Where to go next

- 📘 [Official Airflow documentation](https://airflow.apache.org/docs/apache-airflow/stable/) –
  especially *Core Concepts* and *Best Practices*.
- 🔌 [Providers](https://airflow.apache.org/docs/#providers-packages-docs-apache-airflow-providers-index-html) –
  ready-made operators and hooks for Postgres, AWS, GCP, Snowflake, dbt, Slack…
- 🧪 Testing DAGs with `pytest` and `dag.test()`.
- 🧩 Dynamic task mapping: `my_task.expand(x=[1, 2, 3])` creates one task instance per item.
- ⏳ Deferrable operators & the triggerer – waiting for things without holding a worker slot.
- 🏭 Running Airflow in production: the official Helm chart, Celery / Kubernetes executors.

Turn on the ~50 official examples to explore more patterns: set `AIRFLOW_LOAD_EXAMPLES=true` in
`.env` and run `docker compose up -d`.

🎓 That's it — you've gone from zero to scheduling, passing data, reacting to data, configuring and
debugging pipelines. Happy orchestrating!
