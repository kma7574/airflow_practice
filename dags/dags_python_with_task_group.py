from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.decorators import task_group
from airflow.utils.task_group import TaskGroup

with DAG(
    dag_id="dags_python_with_task_group",
    schedule=None,
    start_date=pendulum.datetime(2023, 4, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    def inner_func(**kwargs):
        msg = kwargs.get('msg') or '' 
        print(msg)

    @task_group(group_id='first_group') # 데코레이터를 사용하여 만든 그룹
    def group_1():
        ''' task_group 데커레이터를 이용한 첫 번째 그룹입니다. '''

        @task(task_id='inner_function1')
        def inner_func1(**kwargs):
            print('첫 번째 TaskGroup 내 첫 번째 task입니다.')

        inner_function2 = PythonOperator(
            task_id='inner_function2',
            python_callable=inner_func,
            op_kwargs={'msg':'첫 번째 TaskGroup내 두 번쨰 task입니다.'}
        )

        inner_func1() >> inner_function2

    with TaskGroup(group_id='second_group', tooltip='두 번째 그룹입니다') as group_2: # 클래스를 이용하여 만든 그룹
        # tooltip 매개변수는 데코레이터를 사용하여 만든 그룹의 docstring과 같음, 때문에 클래스에서의 docstring은 표시되지않음
        ''' 여기에 적은 docstring은 표시되지 않습니다'''
        @task(task_id='inner_function1')
        def inner_func1(**kwargs):
            print('두 번째 TaskGroup 내 첫 번째 task입니다.')

        inner_function2 = PythonOperator(
            task_id='inner_function2',
            python_callable=inner_func,
            op_kwargs={'msg': '두 번째 TaskGroup내 두 번째 task입니다.'}
        )
        inner_func1() >> inner_function2

    group_1() >> group_2