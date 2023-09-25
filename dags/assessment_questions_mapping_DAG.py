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
    pg_hook = PostgresHook(postgres_conn_id='postgres_result_db')
    pg_conn = pg_hook.get_conn()
    pg_cursor = pg_conn.cursor()
    ti = kwargs['ti']
    transform_data_output = ti.xcom_pull(task_ids='transform_data')
    for transform_row in transform_data_output:
        pg_cursor.execute(
            'INSERT INTO assessment_question_mapping (assessment_mcq_mapping_id,'
            'assessment_id,'
            'multiple_choice_question_id)'
            'VALUES (%s,%s,%s)'
            'on conflict (assessment_mcq_mapping_id) do update set assessment_id = EXCLUDED.assessment_id,'
            'multiple_choice_question_id = EXCLUDED.multiple_choice_question_id;',
            (
                transform_row[0],
                transform_row[1],
                transform_row[2],
            )
        )
    pg_conn.commit()

def cleanup_assignment_question_mapping(**kwargs):
    pg_hook_read_replica = PostgresHook(postgres_conn_id='postgres_read_replica')  # Use the read replica connection
    pg_conn_read_replica = pg_hook_read_replica.get_conn()
    pg_cursor_read_replica = pg_conn_read_replica.cursor()

    pg_cursor_read_replica.execute('''
        select 
            id as assessment_mcq_mapping_id
        from
            assessments_Assessmentmultiplechoicequestionmapping
    ''')

    unique_keys = [row[0] for row in pg_cursor_read_replica.fetchall()]
    pg_conn_read_replica.close()

    pg_hook_result_db = PostgresHook(postgres_conn_id='postgres_result_db')
    pg_conn_result_db = pg_hook_result_db.get_conn()
    pg_cursor_result_db = pg_conn_result_db.cursor()

    pg_cursor_result_db.execute(f'''
        DELETE FROM assessment_question_mapping
        WHERE assessment_mcq_mapping_id NOT IN ({','.join(['%s'] * len(unique_keys))})
    ''', unique_keys)

    pg_conn_result_db.commit()


dag = DAG(
    'normal_assessment_question_mapping_dag',
    default_args=default_args,
    description='A DAG for assessment mcq(s) released through normal flow',
    schedule_interval='50 20 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS assessment_question_mapping (
            id serial not null,
            assessment_mcq_mapping_id bigint not null PRIMARY KEY, 
            assessment_id bigint,
            multiple_choice_question_id bigint
        );
    ''',
    dag=dag
)
transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''select 
        id as assessment_mcq_mapping_id,
        assessment_id,
        multiple_choice_question_id
    from
        assessments_assessmentmultiplechoicequestionmapping
    ;
        ''',
    dag=dag
)
extract_python_data = PythonOperator(
    task_id='extract_python_data',
    python_callable=extract_data_to_nested,
    provide_context=True,
    dag=dag
)

cleanup_data = PythonOperator(
    task_id='cleanup_data',
    python_callable=cleanup_assignment_question_mapping,
    provide_context=True,
    dag=dag
)

create_table >> transform_data >> extract_python_data >> cleanup_data