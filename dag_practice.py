from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'DAG',
    'retry': 2,
    'retry_delay': timedelta(seconds=30)
}


def greeting(name, age):
    print(f"My name  is {name}and I am {age}years old")


with DAG(
        dag_id='first_dag_v1'
default_args=default_args,
description='This is our first dag'
start_date=datetime(2023, 5, 20, 3),
schedule_interval='@daily'
) as dag:
    task1 = pythonOperator(
        task_id='first_task_python',
        pythonCallable=addition,
        op_kwargs={name: "Raj", Age: 22}

    )
    task1
