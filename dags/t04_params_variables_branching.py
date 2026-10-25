"""Tutorial 07 - Params, Variables and branching.

* **Params**: a form is shown when you click *Trigger*; values reach tasks via ``params``.
* **Variables**: global key/value config. ``greeting_name`` is defined in docker-compose.yaml
  (``AIRFLOW_VAR_GREETING_NAME``); you can also create Variables in *Admin -> Variables*.
* **Branching**: ``choose_path`` returns the task_id to run next; the other branch is *skipped*.
"""

import pendulum

from airflow.sdk import Param, Variable, dag, task


@dag(
    dag_id="t04_params_variables_branching",
    description="Trigger form, Variables and a branch",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["tutorial"],
    params={
        "language": Param("en", type="string", enum=["en", "fr"], description="Greeting language"),
        "shout": Param(False, type="boolean", description="Upper-case the greeting?"),
    },
    doc_md=__doc__,
)
def params_variables_branching():
    @task.branch
    def choose_path(params=None) -> str:
        return "greet_in_french" if params["language"] == "fr" else "greet_in_english"

    @task
    def greet_in_english(params=None):
        name = Variable.get("greeting_name", default="stranger")
        message = f"Hello, {name}!"
        print(message.upper() if params["shout"] else message)

    @task
    def greet_in_french(params=None):
        name = Variable.get("greeting_name", default="inconnu")
        message = f"Bonjour, {name} !"
        print(message.upper() if params["shout"] else message)

    choose_path() >> [greet_in_english(), greet_in_french()]


params_variables_branching()
