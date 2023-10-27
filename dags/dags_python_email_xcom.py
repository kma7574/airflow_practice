from airflow import DAG
import pendulum
import datetime
from airflow.decorators import task
from airflow.operators.email import EmailOperator

with DAG(
    dag_id="dags_python_email_operator",
    schedule="0 8 1 * *",
    start_date=pendulum.datetime(2023, 10, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:

    @task(task_id='something_task')
    def some_logic():
        from random import choice
        return choice(['Success', 'Failure'])

    send_email = EmailOperator(
        task_id = 'send_email',
        to = 'kma7574@naver.com',
        subject = '{{ data_interval_end.in_timezone("Asia/Seoul") | ds}} some logic 처리 결과',
        html_content = '{{ data_interval_end.in_timezone("Asia/Seoul") | ds}} some logic 처리 결과는 <br> \
                        {{ti.xcom_pull(task_ids="something_task")}} 했습니다. <br> \
                        위 메시지는 airflow email operator를 통해 전송되었습니다. <br>'
    )

    some_logic() >> send_email