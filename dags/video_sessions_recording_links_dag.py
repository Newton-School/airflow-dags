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
                'INSERT INTO video_session_recording_links (video_session_recording_id,'
                'created_at, hash, one_to_one_id, lecture_id, gs_meeting_id,'
                'recording_link, link_duration_in_seconds, link_duration_in_minutes)'
                'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                'on conflict (video_session_recording_id) do update set created_at =EXCLUDED.created_at,'
                'hash =EXCLUDED.hash,'
                'one_to_one_id =EXCLUDED.one_to_one_id,'
                'lecture_id=EXCLUDED.lecture_id,'
                'gs_meeting_id=EXCLUDED.gs_meeting_id,'
                'recording_link=EXCLUDED.recording_link,'
                'link_duration_in_seconds=EXCLUDED.link_duration_in_seconds,'
                'link_duration_in_minutes= EXCLUDED.link_duration_in_minutes;',
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
                 )
        )
    pg_conn.commit()


dag = DAG(
    'video_session_recording_links_dag',
    default_args=default_args,
    description='Recordings links dag for all video sessions',
    schedule_interval='30 20 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS video_session_recording_links (
            id serial,
            video_session_recording_id bigint not null PRIMARY KEY,
            created_at timestamp,
            hash text,
            one_to_one_id bigint,
            lecture_id bigint,
            gs_meeting_id bigint,
            recording_link text,
            link_duration_in_seconds real,
            link_duration_in_minutes real
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''
    select 
        id as video_session_recording_id,
        created_at,
        hash,
        case 
            when video_session_content_type_id = 100 then video_session_object_id 
        end as one_to_one_id,
        case 
            when video_session_content_type_id = 46 then video_session_object_id 
        end as lecture_id,
        case 
            when video_session_content_type_id = 45 then video_session_object_id 
        end as gs_meeting_id,
        recording as recording_link,
        extract(epoch from video_recording_duration) as link_duration_in_seconds,
        extract(epoch from video_recording_duration)/60 as link_duration_in_minutes
    from
        video_sessions_videosessionrecording
    where created_at >= '2022-01-01';
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