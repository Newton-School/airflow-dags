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
            'INSERT INTO group_sessions (table_unique_key, meeting_id, '
            'booked_by_id, mentee_user_id, child_video_session,'
            'course_id, actual_duration, created_at, start_timestamp, end_timestamp, '
            'end_via_api,hash, participants_count, reports_pulled, '
            'title, video_session_using, with_mentees, is_deleted,'
            'deleted_by_id, should_redirect, cancel_reason, meeting_status)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (table_unique_key) do update set meeting_id = EXCLUDED.meeting_id,'
            'booked_by_id = EXCLUDED.booked_by_id,'
            'mentee_user_id = EXCLUDED.mentee_user_id,'
            'child_video_session = EXCLUDED.child_video_session,'
            'course_id = EXCLUDED.course_id,'
            'actual_duration = EXCLUDED.actual_duration,'
            'created_at = EXCLUDED.created_at,'
            'start_timestamp = EXCLUDED.start_timestamp,'
            'end_timestamp = EXCLUDED.end_timestamp,'
            'end_via_api = EXCLUDED.end_via_api,'
            'hash = EXCLUDED.hash,'
            'participants_count = EXCLUDED.participants_count,'
            'reports_pulled = EXCLUDED.reports_pulled,'
            'title = EXCLUDED.title,'
            'video_session_using = EXCLUDED.video_session_using,'
            'with_mentees = EXCLUDED.with_mentees,'
            'is_deleted = EXCLUDED.is_deleted,'
            'deleted_by_id = EXCLUDED.deleted_by_id,'
            'should_redirect = EXCLUDED.should_redirect,'
            'cancel_reason = EXCLUDED.cancel_reason,'
            'meeting_status = EXCLUDED.meeting_status ;',
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
                transform_row[21],
            )
        )
    pg_conn.commit()


dag = DAG(
    'group_sessions_dag',
    default_args=default_args,
    description='Group Sessions mentor and mentee data',
    schedule_interval='35 20 * * *',
    catchup=False,
    max_active_runs=1
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS group_sessions (
            id serial,
            table_unique_key not null text,
            meeting_id int,
            booked_by_id bigint,
            mentee_user_id bigint,
            child_video_session boolean,
            course_id int,
            actual_duration int,
            created_at TIMESTAMP,
            start_timestamp TIMESTAMP,
            end_timestamp TIMESTAMP,
            end_via_api boolean,
            hash varchar(256),
            participants_count int,
            reports_pulled boolean,
            title varchar(256),
            video_session_using int,
            with_mentees boolean,
            is_deleted boolean,
            deleted_by_id bigint,
            should_redirect boolean,
            cancel_reason varchar(256),
            meeting_status int
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''select
                concat(video_sessions_meeting.id, video_sessions_meeting_booked_with.user_id) as table_unique_key,
                video_sessions_meeting.id as meeting_id,
                booked_by_id,
                video_sessions_meeting_booked_with.user_id as mentee_user_id,
                child_video_session,
                course_id,
                actual_duration,
                created_at,
                start_timestamp,
                end_timestamp,
                end_via_api,
                hash,
                participants_count,
                reports_pulled,
                title,
                video_session_using,
                with_mentees,
                is_deleted,
                deleted_by_id,
                should_redirect,
                cancel_reason,
                meeting_status
            from
                video_sessions_meeting
            LEFT JOIN video_sessions_meeting_booked_with
                on video_sessions_meeting_booked_with.meeting_id = video_sessions_meeting.id;
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