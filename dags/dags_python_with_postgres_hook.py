from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator

with DAG(
    dag_id='dags_python_with_postgres',
    start_date=pendulum.datetime(2023,4,1, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:

    
    def insrt_postgres(postgres_conn_id, **kwargs):
        import psycopg2  # python환경에서 postgres sql을 수행하는 라이브러리
        from contextlib import closing
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        postgres_hook = PostgresHook(postgres_conn_id)  # Hook객체를 통해 sql connection정보를 가져옴 
        # cursor 객체를 통해 sql문 수행으로 만들어진 결과에 접근
        with closing(postgres_hook.get_conn()) as conn:  # Hook을 통해 가져온 정보를 conn객체에 전달
            with closing(conn.cursor()) as cursor:
                dag_id = kwargs.get('ti').dag_id
                task_id = kwargs.get('ti').task_id
                run_id = kwargs.get('ti').run_id
                msg = 'insrt 수행'
                sql = 'insert into py_opr_drct_insrt values (%s,%s,%s,%s);'
                cursor.execute(sql,(dag_id,task_id,run_id,msg))
                conn.commit()

    insrt_postgres = PythonOperator(
        task_id='insrt_postgres',
        python_callable=insrt_postgres,
        op_kwargs={'postgres_conn_id':'conn-db-postgres-custom'} # 밖으로 누설되어서는 안되는 정보들을 숨길 수 있음
    )
        
    insrt_postgres