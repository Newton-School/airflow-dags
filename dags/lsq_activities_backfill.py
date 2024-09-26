from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 16),
}

dag = DAG(
    'LSQ_Leads_and_activities_backfiller',
    default_args=default_args,
    description='Backfiller/temp DAG',
    schedule_interval=None,
    catchup=False
)

create_copy = PostgresOperator(
    task_id='create_copy',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE lsq_leads_x_activities_backup as SELECT * FROM lsq_leads_x_activities;
    ''',
    dag=dag
)

drop_table = PostgresOperator(
    task_id='drop_table',
    postgres_conn_id='postgres_result_db',
    sql='''DROP TABLE IF EXISTS lsq_leads_x_activities;
    ''',
    dag=dag
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE lsq_leads_x_activities as SELECT * FROM lsq_leads_x_activities_temp;
    ''',
    dag=dag
)

create_copy >> drop_table >> create_table
