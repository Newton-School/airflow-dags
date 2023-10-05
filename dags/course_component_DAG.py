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
            'INSERT INTO course_component (id,'
            'object_id,'
            'created_at,'
            'content_type_id,'
            'course_id,'
            'parent_id,'
            'is_independent,'
            'optional,'
            'component_type,'
            'deadline_timestamp,'
            'unlock_timestamp,'
            'clearance_points,'
            'title)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (id) do update set object_id = EXCLUDED.object_id,'
            'parent_id = EXCLUDED.parent_id,'
            'is_independent = EXCLUDED.is_independent,'
            'optional = EXCLUDED.optional,'
            'component_type = EXCLUDED.component_type,'
            'deadline_timestamp = EXCLUDED.deadline_timestamp,'
            'unlock_timestamp = EXCLUDED.unlock_timestamp,'
            'clearance_points = EXCLUDED.clearance_points,'
            'title = EXCLUDED.title;',
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
                transform_row[11],
                transform_row[12],
            )
        )
    pg_conn.commit()


dag = DAG(
    'course_component_dag',
    default_args=default_args,
    description='Course component as it is',
    schedule_interval='0 20 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS course_component (
            id int not null PRIMARY KEY,
            object_id bigint,
            created_at timestamp,
            content_type_id int,
            course_id int, 
            parent_id int,
            is_independent boolean,
            optional boolean,
            component_type int,
            deadline_timestamp timestamp,
            unlock_timestamp timestamp,
            clearance_points int,
            title text 
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''
        select 
            id,
            object_id,
            created_at,
            content_type_id,
            course_id,
            parent_id,
            is_independent,
            optional,
            component_type,
            deadline_timestamp,
            unlock_timestamp,
            clearance_points,
            title
        from
            courses_coursecomponent;
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