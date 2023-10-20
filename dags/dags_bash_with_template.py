from airflow import DAG
import pendulum
import datetime
from airflow.operators.bash import BashOperator

with DAG(
    dag_id = "dags_bash_with_template",
    schedule = "10 0 * * *",
    start_date = pendulum.datetime(2023, 10, 1, tz="Asia/Seoul"),
    catchup = False,
) as dag:
    bash_t1 = BashOperator(
        task_id = "bash_t1",
        bash_command = 'echo "data_interval_end: {{ data_interval_end }} "'
    )

    bash_t2 = BashOperator(
        task_id = "bash_t2",
        env = {
            'Start_Date: {{data_interval_start | ds}}',
            'End_Date: {{data_interval_end | ds}}'
        },
        bash_command = 'echo $Start_Date && $End_Date'
    )

    bash_t1 >> bash_t2