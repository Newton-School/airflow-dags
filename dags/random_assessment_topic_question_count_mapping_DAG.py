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
                'INSERT INTO random_assessment_topic_question_count (table_unique_key,'
                'assessment_id,'
                'topic_id,'
                'difficulty_level,'
                'question_count) VALUES (%s,%s,%s,%s,%s)'
                'on conflict (table_unique_key) do update set assessment_id = EXCLUDED.assessment_id,'
                'topic_id = EXCLUDED.topic_id,'
                'difficulty_level = EXCLUDED.difficulty_level,'
                'question_count = EXCLUDED.question_count;',
                (
                    transform_row[0],
                    transform_row[1],
                    transform_row[2],
                    transform_row[3],
                    transform_row[4],
                 )
        )
    pg_conn.commit()


dag = DAG(
    'random_assessment_topic_question_count_dag',
    default_args=default_args,
    description='Random Assessments topic wise question count mapping',
    schedule_interval='30 20 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS random_assessment_topic_question_count (
            id serial not null,
            table_unique_key bigint NOT NULL PRIMARY KEY,
            assessment_id bigint,
            topic_id int,
            difficulty_level int,
            question_count int
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''
    select
        assessments_assessmenttopiclevelnumbermapping.id as table_unique_key,
        assessments_assessment.id as assessment_id,
        assessments_assessmenttopicmapping.topic_id,
        assessments_assessmenttopiclevelnumbermapping.level as difficulty_level,
        assessments_assessmenttopiclevelnumbermapping.number as question_count
    from
        assessments_assessment
    join assessments_assessmenttopicmapping
        on assessments_assessmenttopicmapping.assessment_id = assessments_assessment.id
    join assessments_assessmenttopiclevelnumbermapping
        on assessments_assessmenttopiclevelnumbermapping.assessment_topic_mapping_id = assessments_assessmenttopicmapping.id;
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