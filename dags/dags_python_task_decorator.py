import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import ExternalPythonOperator, PythonVirtualenvOperator, is_venv_installed


with DAG(
    dag_id="dags_python_task_operator",
    schedule="0 2 * * 1",
    start_date=pendulum.datetime(2023, 10, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:

    # [START howto_operator_python]
    @task(task_id="python_task_1")
    def print_context(some_input):
        print(some_input)

    
    python_task_1 = print_context("task_decorator 실행")