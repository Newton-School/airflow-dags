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
    ti = kwargs['ti']
    transform_data_output = ti.xcom_pull(task_ids='transform_data')
    for transform_row in transform_data_output:
        insert_query = f'INSERT INTO user_details_test_3 (user_id,username,email,name,phone,last_login) VALUES ({transform_row[0]},\'{transform_row[1]}\',\'{transform_row[2]}\',\'{transform_row[3]}\', \'{transform_row[4]}\',CAST(\'{transform_row[5]}\' As TIMESTAMP));'
        print(insert_query)
        pg_hook.run(insert_query)


dag = DAG(
    'postgres_transform',
    default_args=default_args,
    description='A DAG for PostgreSQL transformation',
    schedule_interval=None
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS user_details_test_4 (
            id SERIAL PRIMARY KEY,
            user_id bigint not null,
            username varchar(100),
            email varchar(100),
            name varchar(100),
            phone varchar(15),
            last_login timestamp
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''SELECT auth_user.id,username,email,concat(first_name,' ',last_name) as name,users_userprofile.phone,CAST(last_login as VARCHAR) as last_login
            FROM auth_user
            left join users_userprofile on users_userprofile.user_id = auth_user.id;
        ''',
    dag=dag
)

extract_python_data = PythonOperator(
    task_id='extract_python_data',
    python_callable=extract_data_to_nested,
    provide_context=True,
    dag=dag
)

# extract_data = PostgresOperator(
#     task_id='extract_data',
#     postgres_conn_id='postgres_result_db',
#     sql='''SELECT * FROM {{ task_instance.xcom_pull(task_ids='transform_data') }}''',
#     dag=dag
# )

create_table >> transform_data
