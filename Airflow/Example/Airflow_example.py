from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow import DAG


def print_hello(**kwargs):
    variable = kwargs.get('name', 'someone')
    print(f'Hello, {variable}!')


with DAG(
        dag_id ='hello_dag',
        start_date = datetime(2022, 2, 22),
        schedule_interval = '@hourly',
        catchup = False) as dag:

    hello_task = PythonOperator(
        task_id ='hello',
        python_callable = print_hello,
        op_kwargs = {'name': 'Anastasiya'}
    )

hello_task
