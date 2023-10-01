from __future__ import annotations

import random

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule

with DAG(
    dag_id="dags_bash_operator",  # dag_id값이 airflow 화면에서 보여지는 이름(파일명과는 별개지만 dag 이름과 일치시켜주는것이 편함)
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),  # 서울시 기준 2021년 1월 1일부터 시작 
    catchup=False, #True일 때, 과거 누락된 기간까지 포함하여 작업을 할지 결정(보통 False)
    schedule="@daily",
    # schedule="0 0 * * *",  # 분 시 일 월 요일(0시 0분마다 도는 작업) 
) as dag:
    bash_t1 = BashOperator(
        task_id="bash_t1",  # 편리함을 위해 task명도 객체명과 동일하게 설정
        bash_command = "echo whoami"
    )

    bash_t2 = BashOperator(
        task_id="bash_t2",  # 편리함을 위해 task명도 객체명과 동일하게 설정
        bash_command = "echo $HOSTNAME"
    )

    bash_t1 >> bash_t2