from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 16),
}

def extract_data_to_nested(**kwargs):
    def clean_input(data_type, data_value):
        if data_type == 'string':
            return 'null' if not data_value else f'\"{data_value}\"'
        elif data_type == 'datetime':
            return 'null' if not data_value else f'CAST(\'{data_value}\' As TIMESTAMP)'
        else:
            return data_value

    pg_hook = PostgresHook(postgres_conn_id='postgres_result_db')
    pg_conn = pg_hook.get_conn()
    pg_cursor = pg_conn.cursor()
    ti = kwargs['ti']
    transform_data_output = ti.xcom_pull(task_ids='transform_data')
    for transform_row in transform_data_output:
        pg_cursor.execute(
            'INSERT INTO recruiter_details (airbyte_unique_key,name,company,short_intro,'
            'hiring_manager_for_job_link,linkedin_profile_url,airbyte_ab_id,'
            'airbyte_emitted_at,airbyte_normalized_at,airbyte_recruiter_details_hashid)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (linkedin_profile_url) do update set short_intro=EXCLUDED.short_intro,'
            'hiring_manager_for_job_link=EXCLUDED.hiring_manager_for_job_link,company=EXCLUDED.company,'
            ';',
            (
                transform_row[0],
                transform_row[1],
                transform_row[2],
                transform_row[3],
                transform_row[4],
                transform_row[5],
                transform_row[6],
                transform_row[7],
                transform_row[8],
                transform_row[9],
            )
        )
    pg_conn.commit()


dag = DAG(
    'Recruiter_Details_DAG',
    default_args=default_args,
    description='A DAG for Recruiter Details data from Job Posting Analytics DB',
    schedule_interval='30 15 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS recruiter_details (
            id serial,
            airbyte_unique_key varchar(256),
            name varchar(256),
            company varchar(256),
            short_intro varchar(512),
            hiring_manager_for_job_link varchar(1024),
            linkedin_profile_url varchar(256) not null PRIMARY KEY,
            airbyte_ab_id varchar(36),
            airbyte_emitted_at DATE,
            airbyte_normalized_at DATE,
            airbyte_recruiter_details_hashid varchar(32)
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_job_posting',
    sql='''select
            distinct _airbyte_unique_key as airbyte_unique_key,
            name,
            company,
            short_intro,
            hiring_manager_for_job_link,
            linkedin_profile_url,
            _airbyte_ab_id as airbyte_ab_id,
            date(_airbyte_emitted_at) as airbyte_emitted_at,
            date(_airbyte_normalized_at) as airbyte_normalized_at,
            _airbyte_recruiter_details_hashid as airbyte_recruiter_details_hashid
            from recruiter_details;
        ''',
    dag=dag
)

extract_python_data = PythonOperator(
    task_id='extract_python_data',
    python_callable=extract_data_to_nested,
    provide_context=True,
    dag=dag
)

create_table >> transform_data >> extract_python_data