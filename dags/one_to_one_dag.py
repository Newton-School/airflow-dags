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
                'INSERT INTO one_to_one (one_to_one_id, student_user_id, course_id, expert_user_id,'
                'one_to_one_start_timestamp, one_to_one_end_timestamp, hash, one_to_one_created_at,'
                'one_to_one_confirmed_at, one_to_one_cancel_timestamp, one_to_one_status, one_to_one_type,'
                'final_call, cancel_reason, rating, reports_pulled, title,'
                'video_session_using, one_to_one_token_id, difficulty_level, is_ai_enabled)'
                'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                'on conflict (one_to_one_id) do update set student_user_id = EXCLUDED.student_user_id,'
                'course_id = EXCLUDED.course_id,'
                'expert_user_id = EXCLUDED.expert_user_id,'
                'one_to_one_start_timestamp = EXCLUDED.one_to_one_start_timestamp,'
                'one_to_one_end_timestamp = EXCLUDED.one_to_one_end_timestamp,'
                'hash = EXCLUDED.hash,'
                'one_to_one_created_at = EXCLUDED.one_to_one_created_at,'
                'one_to_one_confirmed_at = EXCLUDED.one_to_one_confirmed_at,'
                'one_to_one_cancel_timestamp = EXCLUDED.one_to_one_cancel_timestamp,'
                'one_to_one_status = EXCLUDED.one_to_one_status,'
                'one_to_one_type = EXCLUDED.one_to_one_type,'
                'final_call = EXCLUDED.final_call,'
                'cancel_reason = EXCLUDED.cancel_reason,'
                'rating = EXCLUDED.rating,'
                'reports_pulled = EXCLUDED.reports_pulled,'
                'title = EXCLUDED.title,'
                'video_session_using = EXCLUDED.video_session_using,'
                'one_to_one_token_id = EXCLUDED.one_to_one_token_id,'
                'difficulty_level=EXCLUDED.difficulty_level,'
                'is_ai_enabled = EXCLUDED.is_ai_enabled;',
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
                    transform_row[13],
                    transform_row[14],
                    transform_row[15],
                    transform_row[16],
                    transform_row[17],
                    transform_row[18],
                    transform_row[19],
                    transform_row[20],
                 )
        )
    pg_conn.commit()


dag = DAG(
    'One_to_one_dag',
    default_args=default_args,
    description='One to One Table DAG',
    schedule_interval='35 20 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS one_to_one (
            id serial,
            one_to_one_id bigint not null PRIMARY KEY,
            student_user_id bigint,
            course_id bigint,
            expert_user_id bigint,
            one_to_one_start_timestamp timestamp,
            one_to_one_end_timestamp timestamp,
            hash text,
            one_to_one_created_at timestamp,
            one_to_one_confirmed_at timestamp,
            one_to_one_cancel_timestamp timestamp,
            one_to_one_status int,
            one_to_one_type int,
            final_call int,
            cancel_reason varchar(2048),
            rating int,
            reports_pulled boolean,
            title text,
            video_session_using int,
            one_to_one_token_id bigint,
            difficulty_level int,
            is_ai_enabled boolean
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''select distinct 
        video_sessions_onetoone.id as one_to_one_id,
        video_sessions_onetoone.booked_by_id as student_user_id,
        video_sessions_onetoone.course_id,
        video_sessions_onetoone.booked_with_id as expert_user_id,
        video_sessions_onetoone.start_timestamp as one_to_one_start_timestamp,
        video_sessions_onetoone.end_timestamp as one_to_one_end_timestamp,
        video_sessions_onetoone.hash,
        video_sessions_onetoone.created_at as one_to_one_created_at,
        video_sessions_onetoone.confirmed_at as one_to_one_confirmed_at,
        video_sessions_onetoone.cancel_timestamp as one_to_one_cancel_timestamp,
        video_sessions_onetoone.one_to_one_status,
        video_sessions_onetoone.one_to_one_type,
        video_sessions_onetoone.final_call,
        video_sessions_onetoone.cancel_reason,
        video_sessions_onetoone.rating,
        video_sessions_onetoone.reports_pulled,
        video_sessions_onetoone.title,
        video_sessions_onetoone.video_session_using,
        video_sessions_onetoone.one_to_one_token_id,
        video_sessions_onetoonetoken.difficulty_level,
        video_sessions_onetoone.is_ai_enabled
    from
        video_sessions_onetoone
    left join video_sessions_onetoonetoken
        on video_sessions_onetoonetoken.id = video_sessions_onetoone.one_to_one_token_id;
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