from airflow import DAG
import pendulum
import datetime
from airflow.operators.email import EmailOperator

with DAG(
    dag_id="dags_email_operator",
    schedule="0 8 1 * *",  # 매월 1일 08시
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    send_email_task = EmailOperator(
        task_id='send_email_task',
        to='kma7574@naver.com', # 수신인
        subject='Airflow EmailOperator를 통한 메일',
        html_content='Airflow 작업이 완료되었습니다'
    )