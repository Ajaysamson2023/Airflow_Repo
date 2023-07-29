from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import pythonOperator

default_args = {
    'owner': 'DAG',
    'retry': 2,
    'retry_delay': timedelta(minutes=30)
}


def greeting(ti):
    first_name = ti.xcom_pull(task_ids="getname", key="first_name")
    last_name = ti.xcom_pull(task_ids="getname", key="last_name")
    age = ti.xcom_pull(task_ids="age", key="age")

    print(f"My name  is {first_name}{last_name}and I am {age}years old")


def get_name(ti):
    ti.xcom_push(key="first_name", value="Nivin")
    ti.xcom_push(key="last_name", value="Rakshan")


def get_age(ti):
    ti.xcom_push(key="age", value=22)


with DAG(
        dag_id='first_dag_v1'
default_args=default_args,
description='This is our first dag'
start_date=datetime(2023, 5, 20, 3),
schedule_interval='@daily'
) as dag:
    task1 = pythonOperator(
        task_id='first_task_python',
        pythonCallable=greeting,
    )

    task2 = pythonOperator(
        task_id='second_task_python',
        pythonCallable=get_name,
    )
    task3 = pythonOperator(
        task_id='third_task_python',
        pythonCallable=get_age,
    )
    [task1, task2] >> task3
