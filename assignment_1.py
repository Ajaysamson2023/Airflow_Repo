from datetime import datetime, timedelta
from airflow import DAG

default_args = {
    'owner': 'DAG',
    'retry': 2,
    'retry_delay': timedelta(seconds=30)
}


def addition():
    a = int(input("Enter the a:"))
    b = int(input("Enter the b:"))
    c = a + b
    print("Sum of addition two numbers:", c)


def even_Number():
    a = int(input("Enter the number:"))
    if a % 2 == 0:
        print(a, "it is even")
    else:
        print("it is not even")


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
        dag='dag'
    )
    task2 = pythonOperator(
        task_id='second_task_python',
        pythonCallable=even_Number,
        dag='dag'
    )
    task2 << task1
