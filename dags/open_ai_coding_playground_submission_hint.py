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
            'INSERT INTO open_ai_coding_playground_submission_hint (id,submission_token,'
            'playground_hash,ai_suggestion_response,created_at,submission_status,submission_type)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (id) do nothing ;',
            (
                transform_row[0],
                transform_row[1],
                transform_row[2],
                transform_row[3],
                transform_row[4],
                transform_row[5],
                transform_row[6],
            )
        )
    pg_conn.commit()


dag = DAG(
    'open_ai_coding_playground_submission_hint_dag',
    default_args=default_args,
    description='An Analytics Data Layer DAG for coding playground submission hint from the Data Science Schema',
    schedule_interval='35 18 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS open_ai_coding_playground_submission_hint (
           id int not null PRIMARY KEY,
           submission_token varchar(64),
           playground_hash varchar(256),
           ai_suggestion_response varchar(10000),
           created_at TIMESTAMP,
           submission_status int,
           submission_type int
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_newton_ds',
    sql='''Select 
        id,
       submission_token,
       playground_hash,
       ai_suggestion_response,
       created_at,
       submission_status,
       submission_type
from open_ai_codingplaygroundsubmissionhint;
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