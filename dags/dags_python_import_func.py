import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from common.common_func import get_sftp

with DAG(
    dag_id="dags_python_import_func",  # dag_id값이 airflow 화면에서 보여지는 이름(파일명과는 별개지만 dag 이름과 일치시켜주는것이 편함)
    start_date=pendulum.datetime(2023, 10, 1, tz="Asia/Seoul"),  # 서울시 기준 2021년 1월 1일부터 시작 
    catchup=False, #True일 때, 과거 누락된 기간까지 포함하여 작업을 할지 결정(보통 False)
    schedule="30 6 * * *",
) as dag:
    task_get_sftp = PythonOperator(
        task_id='task_get_sftp',
        python_callable=get_sftp
    )