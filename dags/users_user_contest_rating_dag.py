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
            'INSERT INTO users_contest_rating (users_contest_rating_id, user_id, student_name,'
            'email, course_id, course_name,'
            'created_at, rating, rating_delta,'
            'rank)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (users_contest_rating_id) do update set course_name = EXCLUDED.course_name,'
            'rating = EXCLUDED.rating,'
            'rating_delta = EXCLUDED.rating_delta,'
            'rank = EXCLUDED.rank;',
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
            )
        )
    pg_conn.commit()


dag = DAG(
    'user_contest_rating_dag',
    default_args=default_args,
    description='read replica view of users contest rating table',
    schedule_interval='30 19 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS users_contest_rating (
            id serial,
            users_contest_rating_id bigint not null PRIMARY KEY,
            user_id bigint,
            student_name text,
            email text,
            course_id int, 
            course_name text,
            created_at timestamp,
            rating int,
            rating_delta int,
            rank int 
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''
        select
            users_usercontestrating.id as users_contest_rating_id,
            user_id, 
            concat(auth_user.first_name,' ', auth_user.last_name) as student_name,
            auth_user.email,
            course_id,
            title as course_name,
            users_usercontestrating.created_at,
            rating,
            rating_delta,
            users_usercontestrating.rank
        from
            users_usercontestrating
        join courses_course
            on courses_course.id = users_usercontestrating.course_id 
        join auth_user
            on auth_user.id = users_usercontestrating.user_id;
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