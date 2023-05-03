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
            'INSERT INTO assignment_random_question_mapping (table_unique_key,user_id,course_id,assignment_id,question_id)'
            'VALUES (%s,%s,%s,%s,%s)'
            'on conflict (table_unique_key) do update set course_id = EXCLUDED.course_id,'
            'assignment_id=EXCLUDED.assignment_id,question_id=EXCLUDED.question_id,user_id=EXCLUDED.user_id ;',
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
    'assignment_random_questions_released_DAG',
    default_args=default_args,
    description='A DAG for assignment questions released though the random flow',
    schedule_interval='35 4 * * *',
    catchup=False
)
create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS assignment_random_question_mapping (
            id serial not null,
            table_unique_key double precision not null PRIMARY KEY,
            user_id bigint,
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
                distinct cast(concat(assignments_assignment.id, row_number() over(order by assignments_assignment.id)) as double precision) as table_unique_key,
                courses_courseusermapping.user_id,
                courses_course.id as course_id,
                assignments_assignment.id  as assignment_id,
                assignments_assignmentcourseuserrandomassignedquestionmapping.assignment_question_id as question_id
            from
                assignments_assignment
            left join courses_course 
                on courses_course.id = assignments_assignment.course_id
            left join courses_courseusermapping on courses_courseusermapping.course_id = courses_course.id
            left join assignments_assignmentcourseuserrandomassignedquestionmapping 
                on assignments_assignmentcourseuserrandomassignedquestionmapping.course_user_mapping_id = courses_courseusermapping.id
                and assignments_assignmentcourseuserrandomassignedquestionmapping.assignment_id = assignments_assignment.id
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
create_table >> transform_data >> extract_python_data