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
                'INSERT INTO assignment_topic_difficulty_number_mapping (assignment_topic_diff_mapping_id,'
                'assignment_id,'
                'course_id,'
                'start_timestamp,'
                'end_timestamp,'
                'difficulty_type,'
                'difficulty_level,'
                'topic_id,'
                'question_count)'
                'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                'on conflict (assignment_topic_diff_mapping_id) do update set start_timestamp = EXCLUDED.start_timestamp,'
                'end_timestamp = EXCLUDED.end_timestamp,'
                'difficulty_type = EXCLUDED.difficulty_type,'
                'difficulty_level = EXCLUDED.difficulty_level,'
                'topic_id = EXCLUDED.topic_id,'
                'question_count = EXCLUDED.question_count;',
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
                 )
        )
    pg_conn.commit()


dag = DAG(
    'courses_dag',
    default_args=default_args,
    description='Courses Details, a version of courses_course',
    schedule_interval='30 20 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS assignment_topic_difficulty_number_mapping (
            id serial,
            assignment_topic_diff_mapping_id not null PRIMARY KEY,
            assignment_id int,
            course_id int,
            start_timestamp timestamp,
            end_timestamp timestamp,
            difficulty_type int,
            difficulty_level text,
            topic_id int,
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
        assignments_assignmenttopicdifficultynumbermapping.id as assignment_topic_diff_mapping_id,
        assignments_assignment.id as assignment_id,
        assignments_assignment.course_id,
        assignments_assignment.start_timestamp,
        assignments_assignment.end_timestamp,
        assignments_assignmenttopicdifficultynumbermapping.difficulty_type,
        case
            when assignments_assignmenttopicdifficultynumbermapping.difficulty_type = 1 then 'Beginner'
            when assignments_assignmenttopicdifficultynumbermapping.difficulty_type = 2 then 'Easy'
            when assignments_assignmenttopicdifficultynumbermapping.difficulty_type = 3 then 'Medium'
            when assignments_assignmenttopicdifficultynumbermapping.difficulty_type = 4 then 'Hard'
            when assignments_assignmenttopicdifficultynumbermapping.difficulty_type = 5 then 'Challenge'
        end as difficulty_level,
        assignments_assignmenttopicmapping.topic_id,
        assignments_assignmenttopicdifficultynumbermapping.number as question_count
    from
        assignments_assignment
    join assignments_assignmenttopicmapping
        on assignments_assignmenttopicmapping.assignment_id = assignments_assignment.id and assignments_assignment.hidden = false 
    join assignments_assignmenttopicdifficultynumbermapping
        on assignments_assignmenttopicdifficultynumbermapping.assignment_topic_mapping_id = assignments_assignmenttopicmapping.id;
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