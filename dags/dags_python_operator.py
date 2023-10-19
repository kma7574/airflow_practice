import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator

import random

with DAG(
    dag_id="dags_python_operator",  # dag_id값이 airflow 화면에서 보여지는 이름(파일명과는 별개지만 dag 이름과 일치시켜주는것이 편함)
    start_date=pendulum.datetime(2023, 10, 1, tz="Asia/Seoul"),  # 서울시 기준 2023년 10월 1일부터 시작 
    catchup=False, #True일 때, 과거 누락된 기간까지 포함하여 작업을 할지 결정(보통 False)
    schedule="30 6 * * *",
) as dag:
    def select_fruit():  # 파이썬 함수 실행을 위한 사용자 정의 함수 
        fruit = ['Apple', 'Banana', 'Orange', 'Avocado']
        rand_int = random.randint(0,3)
        print(fruit[rand_int])

    py_t1 = PythonOperator(
        task_id = 'py_t1',
        python_callable=select_fruit  # pythonoperator를 통해 실행시킬 함수
    )

    py_t1