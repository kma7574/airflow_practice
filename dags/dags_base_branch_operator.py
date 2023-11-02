from airflow import DAG
import pendulum
from airflow.operators.branch import BaseBranchOperator
from airflow.operators.python import PythonOperator

with DAG(
    dag_id='dags_base_branch_operator',
    start_date=pendulum.datetime(2023, 11, 1, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:
    
    class CustomBranchOperator(BaseBranchOperator):  # BaseBranchOperator클래스를 상속받아 사용자정의branch_operator 정의
        def choose_branch(self, context):  # def choose_branch(self, context) 구문은 가이드대로 그대로 사용하는 것을 권장, choose_branch를 오버라이딩하여 사용
            import random
            print(context)
            print(type(context))
            
            item_lst = ['A', 'B', 'C']
            selected_item = random.choice(item_lst)
            if selected_item == 'A':
                return 'task_a'
            elif selected_item in ['B','C']:
                return ['task_b','task_c']

    
    custom_branch_operator = CustomBranchOperator(task_id='python_branch_task')  # 클래스 인스턴스

    
    def common_func(**kwargs):
        print(kwargs['selected'])

    task_a = PythonOperator(
        task_id='task_a',
        python_callable=common_func,
        op_kwargs={'selected':'A'}
    )

    task_b = PythonOperator(
        task_id='task_b',
        python_callable=common_func,
        op_kwargs={'selected':'B'}
    )

    task_c = PythonOperator(
        task_id='task_c',
        python_callable=common_func,
        op_kwargs={'selected':'C'}
    )

    custom_branch_operator >> [task_a, task_b, task_c]