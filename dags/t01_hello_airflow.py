"""Tutorial 03 - your very first DAG.

Three tasks in a line:  say_hello  ->  print_date  ->  say_goodbye

No schedule: it only runs when you press the Trigger button.
"""

import pendulum

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def _say_goodbye():
    print("Goodbye from a Python function! Look for me in the task logs.")


with DAG(
    dag_id="t01_hello_airflow",
    description="Three tasks in a row: the smallest useful DAG",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["tutorial"],
    doc_md=__doc__,
) as dag:
    say_hello = BashOperator(
        task_id="say_hello",
        bash_command='echo "Hello, Airflow!"',
    )

    print_date = BashOperator(
        task_id="print_date",
        bash_command="date",
    )

    say_goodbye = PythonOperator(
        task_id="say_goodbye",
        python_callable=_say_goodbye,
    )

    say_hello >> print_date >> say_goodbye
