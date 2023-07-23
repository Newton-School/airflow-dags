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
                'INSERT INTO courses (course_id,course_name,unit_type,course_structure_id,course_structure_name,course_structure_class,course_start_timestamp,course_end_timestamp,course_type,hash,created_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                'on conflict (course_id) do update set course_name=EXCLUDED.course_name,course_structure_id =EXCLUDED.course_structure_id,course_structure_name = EXCLUDED.course_structure_name,course_structure_class =EXCLUDED.course_structure_class,course_start_timestamp=EXCLUDED.course_start_timestamp,course_end_timestamp=EXCLUDED.course_end_timestamp ;',
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
    sql='''CREATE TABLE IF NOT EXISTS courses (
            course_id bigint not null PRIMARY KEY,
            course_name varchar(100),
            unit_type varchar(16),
            course_structure_id int,
            course_structure_name varchar(100),
            course_structure_class varchar(30),
            course_start_timestamp timestamp,
            course_end_timestamp timestamp,
            course_type int,
            hash varchar(100),
            created_at timestamp
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''select
    distinct courses_course.id as course_id,
    courses_course.title as course_name,
    courses_course.unit_type,
    courses_course.course_structure_id,
    courses_coursestructure.title as course_structure_name,
    case 
        when courses_course.course_structure_id in (1,18) then 'PAP'
        when courses_course.course_structure_id in (8,22,23) then 'Upfront - MIA'
        when courses_course.course_structure_id in (14,20) then 'Data Science - Certification'
        when courses_course.course_structure_id in (11,26) then 'Data Science - IU'
        when courses_course.course_structure_id in (6,12,19) then 'Upfront - FSD'
        when courses_course.course_structure_id in (32) then 'Upfront - FSD: Instructor - 2'
    end as course_structure_class,
    courses_course.start_timestamp as course_start_timestamp,
    courses_course.end_timestamp as course_end_timestamp,
    courses_course.course_type,
    courses_course.hash,
    courses_course.created_at as created_at
from
    courses_course
left join courses_coursestructure
    on courses_coursestructure.id = courses_course.course_structure_id;
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