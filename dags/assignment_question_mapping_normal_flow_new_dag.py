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
def update_deleted_mappings(**kwargs):
    def clean_input(data_type, data_value):
        if data_type == 'string':
            return 'null' if not data_value else f'\"{data_value}\"'
        elif data_type == 'datetime':
            return 'null' if not data_value else f'CAST(\'{data_value}\' As TIMESTAMP)'
        else:
            return data_value

    pg_hook = PostgresHook(postgres_conn_id='postgres_read_replica')
    pg_conn = pg_hook.get_conn()
    pg_cursor = pg_conn.cursor()

    result_hook = PostgresHook(postgres_conn_id='postgres_result_db')
    result_conn = result_hook.get_conn()
    result_cursor = result_conn.cursor()

    result_cursor.execute("""
        select distinct table_unique_key from assignment_question_mapping;
            """)
    existing_mappings = set(row[0] for row in result_cursor.fetchall())

    transform_data_output = kwargs['ti'].xcom_pull(task_ids='transform_data')
    new_mappings = set(str(transform_row[0]) for transform_row in transform_data_output)

    # Identify deleted mappings and update is_deleted column
    for deleted_mapping in existing_mappings - new_mappings:
        pg_cursor.execute(
            'UPDATE assignment_question_mapping SET is_deleted = TRUE WHERE table_unique_key = %s;',
            (deleted_mapping,)
        )

    pg_conn.commit()

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
            table_unique_key double precision not null PRIMARY KEY, 
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
            distinct cast(concat(assignments_assignment.id, courses_course.id, aaq.id) as double precision) as table_unique_key,
            courses_course.id as course_id,
            assignments_assignment.id as assignment_id,
            aaq.id as question_id,
            null as is_deleted
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

update_deleted_mappings_task = PythonOperator(
    task_id='update_deleted_mappings',
    python_callable=update_deleted_mappings,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
create_table >> transform_data >> update_deleted_mappings_task

