from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
from airflow.decorators import task


with DAG(
    dag_id="dags_python_template",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2023, 10, 20, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    # 1. op_kargs에 jinja템플릿 변수를 직접 넣어서 실제 값으로 변환
    def python_function1(start_date, end_date, **kwargs):
        print(start_date)
        print(end_date)

    python_t1 = PythonOperator(
        task_id='python_t1',
        python_callable=python_function1,
        op_kwargs={'start_date':'{{data_interval_start | ds}}', 'end_date':'{{data_interval_end | ds}}'}
    )


    # 2. op_kargs에 여러 템플릿 변수들이 이미 정의되어 있으니 그냥 꺼내쓰는 방식
    @task(task_id='python_t2')
    def python_function2(**kwargs):
        print(kwargs)
        print('ds:' + kwargs['ds'])
        print('ts:' + kwargs['ts'])
        print('data_interval_start:' + str(kwargs['data_interval_start']))
        print('data_interval_end:' + str(kwargs['data_interval_end']))
        print('task_instance:' + str(kwargs['ti']))


    # 두 task는 동일한 결과를 출력하도록 수행되지만 그 방식이 다름
    python_t1 >> python_function2()