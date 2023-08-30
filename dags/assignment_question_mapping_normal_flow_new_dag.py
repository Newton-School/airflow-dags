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
            'INSERT INTO assignment_question_mapping (table_unique_key,'
            'course_id,'
            'assignment_id,'
            'question_id)'
            'VALUES (%s,%s,%s,%s)'
            'on conflict (table_unique_key) do update set course_id = EXCLUDED.course_id,'
            'assignment_id=EXCLUDED.assignment_id,'
            'question_id=EXCLUDED.question_id ;',
            (
                transform_row[0],
                transform_row[1],
                transform_row[2],
                transform_row[3],
            )
        )
    pg_conn.commit()

def cleanup_assignment_question_mapping(**kwargs):
    pg_hook_read_replica = PostgresHook(postgres_conn_id='postgres_read_replica')  # Use the read replica connection
    pg_conn_read_replica = pg_hook_read_replica.get_conn()
    pg_cursor_read_replica = pg_conn_read_replica.cursor()

    pg_cursor_read_replica.execute('''
        SELECT distinct concat(assignments_assignment.id, courses_course.id, aaq.id) as table_unique_key
        FROM assignments_assignment
        JOIN courses_course ON courses_course.id = assignments_assignment.course_id
        JOIN assignments_assignmentquestionmapping aaqm ON aaqm.assignment_id = assignments_assignment.id
        JOIN assignments_assignmentquestion aaq ON aaq.id = aaqm.assignment_question_id
    ''')

    unique_keys = [row[0] for row in pg_cursor_read_replica.fetchall()]
    pg_conn_read_replica.close()

    pg_hook_result_db = PostgresHook(postgres_conn_id='postgres_result_db')
    pg_conn_result_db = pg_hook_result_db.get_conn()
    pg_cursor_result_db = pg_conn_result_db.cursor()

    pg_cursor_result_db.execute(f'''
        DELETE FROM assignment_question_mapping_new_logic
        WHERE table_unique_key NOT IN ({','.join(['%s'] * len(unique_keys))})
    ''', unique_keys)

    pg_conn_result_db.commit()


dag = DAG(
    'new_assignment_questions_released_normal_flow_DAG',
    default_args=default_args,
    description='A DAG for assignment questions released through the normal flow',
    schedule_interval='30 20 * * FRI',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS assignment_question_mapping_new_logic (
            id serial not null,
            table_unique_key text not null PRIMARY KEY, 
            course_id bigint,
            assignment_id bigint,
            question_id bigint
        );
    ''',
    dag=dag
)
transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''select
            distinct concat(assignments_assignment.id, courses_course.id, aaq.id) as table_unique_key,
            courses_course.id as course_id,
            assignments_assignment.id as assignment_id,
            aaq.id as question_id
        from 
            assignments_assignment
        join courses_course
            on courses_course.id = assignments_assignment.course_id
        join assignments_assignmentquestionmapping aaqm
            on aaqm.assignment_id = assignments_assignment.id
        join assignments_assignmentquestion aaq
            on aaq.id = aaqm.assignment_question_id
        order by 2,3
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