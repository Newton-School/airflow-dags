from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def print_hello():
	return 'Hello World from first Airflow DAF!'


def print_second_hello():
	return 'Hello Second World from first Airflow DAF!'


hello_dag = DAG('hello_world', description='Hello World DAG', schedule_interval='0 12 * * *', start_date=datetime(2017, 3, 20), catchup=False)

first_hello_operator = PythonOperator(task_id='first_hello', python_callable=print_hello, dag=hello_dag)

second_hello_operator = PythonOperator(task_id='second_hello', python_callable=print_second_hello, dag=hello_dag)

first_hello_operator >> second_hello_operator
