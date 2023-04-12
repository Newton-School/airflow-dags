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
                'INSERT INTO weekly_user_details (course_user_mapping_id,user_id,'
                'admin_course_user_mapping_id,week_view,course_id,unit_type,'
                'status,label_mapping_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s);',
                (
                    transform_row[0],
                    transform_row[1],
                    transform_row[2],
                    transform_row[3],
                    transform_row[4],
                    transform_row[5],
                    transform_row[6],
                    transform_row[7],
                 )
        )
        print(pg_cursor.query)
    pg_conn.commit()


dag = DAG(
    'weekly_user_details_dag',
    default_args=default_args,
    description='A DAG for maintaining WoW user data (user_id + course_id)',
    schedule_interval=None
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS weekly_user_details (
            id serial not null PRIMARY KEY,
            course_user_mapping_id bigint not null,
            user_id bigint,
            admin_course_user_mapping_id bigint,
            week_view timestamp,
            course_id bigint,
            unit_type varchar(16),
            status int,
            label_mapping_id bigint
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''select 
    courses_courseusermapping.id as course_user_mapping_id,
    courses_courseusermapping.user_id,
    courses_courseusermapping.admin_course_user_mapping_id,
    cast(date_trunc('week', cast(now() as date)) as varchar) as week_view,
    courses_course.id as course_id,
    courses_course.unit_type,
    courses_courseusermapping.status,
    courses_courseuserlabelmapping.id as label_mapping_id
from
    courses_courseusermapping
left join courses_course 
    on courses_course.id = courses_courseusermapping.course_id
left join courses_courseuserlabelmapping on courses_courseuserlabelmapping.course_user_mapping_id = courses_courseusermapping.id and label_id =677;
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
