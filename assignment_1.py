from datetime import datetime,timedelta
from airflow import DAG


default_args={
    'owner':'DAG_2',
   'retry':2,
    'retry_delay':timedelta(minute=4)
}

with DAG(
    dag_id='first_dag_v1'
    default_args=default_args,
    description='This is our first dag'
    start_date=datetime(2023,5,20,3),
    schedule_interval='@daily'
)as dag:
    task1=pythonOperator(
        task_id='first_task_python',
        pythonCallable=
        dag='dag'
    )