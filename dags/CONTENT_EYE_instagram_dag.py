from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 18),
}


def extract_recent_posts(**kwargs):
    return [{"hello": "world"}]


def post_on_slack(**kwargs):
    ti = kwargs['ti']
    transform_data_output = ti.xcom_pull(task_ids='extract_instagram_data')
    print(transform_data_output)


dag = DAG(
        'CONTENT_EYE_instagram_dag',
        default_args=default_args,
        description='A DAG to get the latest posts from the official Instagram accounts',
        schedule_interval='10 * * * *',
        catchup=False
)

extract_instagram_data = PythonOperator(
        task_id='extract_instagram_data',
        python_callable=extract_recent_posts,
        provide_context=True,
        dag=dag
)
post_on_slack = PythonOperator(
        task_id='post_on_slack',
        python_callable=post_on_slack,
        provide_context=True,
        dag=dag
)

extract_instagram_data >> post_on_slack
