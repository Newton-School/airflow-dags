from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 16),
}

dag = DAG(
    'postgres_transform',
    default_args=default_args,
    description='A DAG for PostgreSQL transformation',
    schedule_interval=None
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS user_details_test (
            id SERIAL PRIMARY KEY,
            user_id bigint not null,
            date_joined DATE NOT NULL,
            username varchar(100),
            email varchar(100),
            name varchar(100),
            last_login Date,
            phone bigint
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''SELECT auth_user.id,username,email,concat(first_name,' ',last_name) as name,users_userprofile.phone
            FROM auth_user
            left join users_userprofile on users_userprofile.user_id = auth_user.id;
        ''',
    dag=dag
)

extract_data = PostgresOperator(
    task_id='extract_data',
    postgres_conn_id='postgres_result_db',
    sql='''SELECT * FROM {{ task_instance.xcom_pull(task_ids='transform_data') }}''',
    dag=dag
)

create_table >> transform_data >> extract_data
