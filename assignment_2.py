from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


default_args={
    'owner':'DAG_1',
   'retry':5,
    'retry_delay':timedelta(minute=2)
}

with DAG(
    dag_id='first_dag_v1'
    default_args=default_args,
    description='This is our first dag'
    start_date=datetime(2022,8,28,2),
    schedule_interval='@daily'
)as dag:
    task1=BashOperator(
        task_id='first_task',
        bash_command='echo,this is task1 will run first'
        dag='dag'

    )
    task2 = BashOperator(
        task_id='second_task',
        bash_command='echo hey,this is task2 will be running after task1'
        dag = 'dag'

    )
    task3 = BashOperator(
        task_id='third_task',
        bash_command='echo hey,this is task3 will be running after Task1 and Task2'
        dag = 'dag'

    )

    #task1.setDownstream(task2)
    #task1.setDownstream(task3)
    task1 >> task2
    task1 >> task3
    # task1.setUpstream(task2)
    # task1.setUpstream(task3)




