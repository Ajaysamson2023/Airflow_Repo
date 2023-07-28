from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "DAG",
    "retries": 1,
    "retry_delay": timedelta(seconds=30)
}


def list_of_values():
    list_1 = [1, 4, 5, 6]
    print(list_1)


def strings():
    print("This is the first dag with PythonOperator")
    words = "welcome to python"
    count = 0
    for i in words:
        if i == "e":
            count += 1
    print(str(count))


def dictionary():
    user = {
        "name": "raj",
        "age": 23,
        "is married": True
    }
    print(user.keys())
    print(user.values())
    print(user.items())


with DAG(dag_id="dag_version_1",
         start_date=datetime(2022, 5, 22),
         schedule_interval="@daily",
         default_args=default_args,
         catchup=False
         ) as dag:
    task1 = PythonOperator(task_id="task1",
                           python_callable=strings,
                           dag=dag)
    task2 = PythonOperator(task_id="task2",
                           python_callable=list_of_values,
                           dag=dag)
    task3 = PythonOperator(task_id="task2",
                           python_callable=dictionary,
                           dag=dag)
    task1 >> [task2, task3]
