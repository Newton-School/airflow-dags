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
            'INSERT INTO recorded_lectures_course_user_reports (table_unique_key,lecture_id,course_user_mapping_id,total_time_watched_in_mins)'
            'VALUES (%s,%s,%s,%s)'
            'on conflict (table_unique_key) do update set lecture_id = EXCLUDED.lecture_id,'
            'course_user_mapping_id = EXCLUDED.course_user_mapping_id,'
            'total_time_watched_in_mins = EXCLUDED.total_time_watched_in_mins;',
            (
                transform_row[0],
                transform_row[1],
                transform_row[2],
                transform_row[3]
            )
        )
    pg_conn.commit()


dag = DAG(
    'recorded_lectures_dag',
    default_args=default_args,
    description='Per lecture per user recorded lectures time',
    schedule_interval='30 5 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS recorded_lectures_course_user_reports (
            id serial not null,
            table_unique_key double precision not null PRIMARY KEY,
            lecture_id bigint,
            course_user_mapping_id bigint,
            total_time_watched_in_mins bigint
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''select
    concat(video_sessions_lecturecourseuserreport.lecture_id, row_number() over(order by lecture_id)) as table_unique_key,
    lecture_id,
    course_user_mapping_id,
    sum(duration) / 60 as total_time_watched_in_mins
from
    video_sessions_lecturecourseuserreport
group by 2,3;
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