# Package Import
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum

with DAG(
    dag_id='dags_trigger_dag_run_operator',
    start_date=pendulum.datetime(2023, 11, 1, tz='Asia/Seoul'),
    schedule='30 9 * * *',
    catchup=False
) as dag:

    start_task = BashOperator(
        task_id='start_task',
        bash_command='echo "start!"',
    )

    trigger_dag_task = TriggerDagRunOperator(
        task_id='trigger_dag_task',
        trigger_dag_id='dags_python_operator',     # 호출할 DAG_ID 입력, task_id와 trigger_dag_id는 필수 매개변수
        trigger_run_id=None,                       # 실행중인 run_id를 입력하면 해당 run_id를 실행
        execution_date='{{data_interval_start}}',  # 실행 날짜를 지정
        reset_dag_run=True,                        # 해당 dag에 dag_run이 있을 경우 초기화 하고 다시 시작
        wait_for_completion=False,                 # DAG_run이 완료 될때 까지 기다림
        poke_interval=60,                          # wait_for_completion을 true로 했다면, Trigger가 작동했는지 확인하는 간격을 지정 할 수 있다. default는 60
        allowed_states=['success'],                # Trigger를 실행할 state. list로 작성해야하며 default값은 success
        failed_states=None
        )

    start_task >> trigger_dag_task