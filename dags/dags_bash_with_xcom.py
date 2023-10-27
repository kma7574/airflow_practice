from airflow import DAG
import pendulum
import datetime
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_xcom",
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2023, 10, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    # bash operator에서 template문법 사용이 가능한 파라미터는 bash_Command, env
    # kargs에서 ti 객체를 꺼내와서 push,pull했던 python operator와 달리 
    # bash operator에서는 꺼내올 필요 없이 ti.xcom_push 처럼 사용 가능
    bash_push = BashOperator(
    task_id='bash_push',
    bash_command="echo START && "
                 "echo XCOM_PUSHED "
                 "{{ ti.xcom_push(key='bash_pushed',value='first_bash_message') }} && "
                 "echo COMPLETE"
    )
    # 출력되는 마지막 문장이 return value로 간주(위 operator에서는 COMPLETE가 해당)

    bash_pull = BashOperator(
        task_id='bash_pull',
        env={'PUSHED_VALUE':"{{ ti.xcom_pull(key='bash_pushed') }}",
            'RETURN_VALUE':"{{ ti.xcom_pull(task_ids='bash_push') }}"},
        bash_command="echo $PUSHED_VALUE && echo $RETURN_VALUE ",
        do_xcom_push=False
    )

    bash_push >> bash_pull