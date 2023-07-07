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
    'coding_playground_submission_DAG',
    default_args=default_args,
    description='A DAG for coding playground submission',
    schedule_interval='0 5 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS coding_playground_submissions (
            coding_playground_submission_id bigint not null PRIMARY KEY,
            coding_playground_submission_hash varchar(256),
            created_at TIMESTAMP,
            coding_playground_id bigint,
            user_id bigint,
            all_test_cases_passing boolean,
            compilation_error boolean,
            number_of_test_cases_passing int,
            wrong_submission boolean,
            assignment_id int,
            assignment_question_id int
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''
        Select 
        id as coding_playground_submission_id,
       hash as coding_playground_submission_hash, 
       created_at,
       coding_playground_id,
       user_id,
       all_test_cases_passing,
       compilation_error,
       number_of_test_cases_passing,
       wrong_submission,
       assignment_id,
       assignment_question_id
        from playgrounds_codingplaygroundsubmission;
        ''',
    dag=dag
)


def extract_exception_logs(**kwargs):
    transform_data_output = kwargs['ti'].xcom_pull(task_ids=extract_python_data)
    for logs in transform_data_output:
        print(logs)


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
            'INSERT INTO coding_playground_submissions (coding_playground_submission_id,'
            'coding_playground_submission_hash,created_at,coding_playground_id,user_id,'
            'all_test_cases_passing,compilation_error,number_of_test_cases_passing,'
            'wrong_submission,assignment_id,assignment_question_id) '
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (coding_playground_submission_id) do nothing ;',
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
                transform_row[10],
            )
        )
        pg_conn.commit()


extract_python_data = PythonOperator(
    task_id='extract_python_data',
    python_callable=extract_data_to_nested,
    provide_context=True,
    dag=dag
)


create_table >> transform_data >> extract_python_data
