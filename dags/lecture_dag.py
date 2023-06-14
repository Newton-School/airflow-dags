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
                'INSERT INTO lectures (lecture_id,lecture_title,course_id,child_video_session,created_by_id,created_at,start_timestamp,end_timestamp,'
                'hash,mandatory,video_session_using,instructor_user_id,lecture_slot_id,is_topic_tree_independent,lecture_slot_status,'
                'lecture_slot_is_deleted,lecture_slot_created_at,lecture_slot_created_by_id,'
                'lecture_slot_deleted_by_id,lecture_slot_modified_at,automated_content_release_triggered,lecture_type) '
                'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                'on conflict (lecture_id) do update set mandatory =EXCLUDED.mandatory,'
                'lecture_title =EXCLUDED.lecture_title,'
                'child_video_session =EXCLUDED.child_video_session,'
                'lecture_slot_status=EXCLUDED.lecture_slot_status,'
                'start_timestamp=EXCLUDED.start_timestamp,'
                'end_timestamp=EXCLUDED.end_timestamp,'
                'lecture_slot_is_deleted=EXCLUDED.lecture_slot_is_deleted,'
                'lecture_slot_created_by_id= EXCLUDED.lecture_slot_created_by_id,'
                'lecture_slot_deleted_by_id= EXCLUDED.lecture_slot_deleted_by_id,'
                'lecture_slot_modified_at=EXCLUDED.lecture_slot_modified_at,'
                'instructor_user_id =EXCLUDED.instructor_user_id,'
                'lecture_slot_id =EXCLUDED.lecture_slot_id,'
                'automated_content_release_triggered=EXCLUDED.automated_content_release_triggered,'
                'lecture_type=EXCLUDED.lecture_type ;',
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
    'Lectures_dag',
    default_args=default_args,
    description='Lectures Info Table DAG',
    schedule_interval='30 20 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS lectures (
            lecture_id bigint not null PRIMARY KEY,
            lecture_title varchar(128),
            course_id bigint,
            child_video_session boolean,
            created_by_id bigint,
            created_at timestamp,
            start_timestamp timestamp,
            end_timestamp timestamp,
            hash varchar(100),
            mandatory boolean,
            video_session_using int,
            instructor_user_id bigint,
            lecture_slot_id bigint,
            is_topic_tree_independent boolean,
            lecture_slot_status int,
            lecture_slot_is_deleted boolean,
            lecture_slot_created_at timestamp,
            lecture_slot_created_by_id bigint,
            lecture_slot_deleted_by_id bigint,
            lecture_slot_modified_at timestamp,
            automated_content_release_triggered boolean,
            lecture_type varchar(256)
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''select
            video_sessions_lecture.id as lecture_id,
            video_sessions_lecture.title as lecture_title,
            video_sessions_lecture.course_id,
            video_sessions_lecture.child_video_session,
            video_sessions_lecture.created_by_id,
            cast(video_sessions_lecture.created_at as varchar) as created_at,
            cast(video_sessions_lecture.start_timestamp as varchar) as start_timestamp,
            cast(video_sessions_lecture.end_timestamp as varchar) as end_timestamp,
            video_sessions_lecture.hash,
            video_sessions_lecture.mandatory,
            video_sessions_lecture.video_session_using, 
            video_sessions_lecture.instructor_user_id,
            video_sessions_lectureslot.id as lecture_slot_id,
            video_sessions_lectureslot.is_topic_tree_independent,
            video_sessions_lectureslot.status as lecture_slot_status,
            video_sessions_lectureslot.is_deleted as lecture_slot_is_deleted,
            cast(video_sessions_lectureslot.created_at as varchar) as lecture_slot_created_at,
            video_sessions_lectureslot.created_by_id as lecture_slot_created_by_id,
            video_sessions_lectureslot.deleted_by_id as lecture_slot_deleted_by_id,
            cast(video_sessions_lectureslot.modified_at as varchar) as lecture_slot_modified_at,
            video_sessions_lectureslot.automated_content_release_triggered,
            technologies_label.name as lecture_type
            from video_sessions_lecture
            left join video_sessions_lectureslot on video_sessions_lectureslot.lecture_id = video_sessions_lecture.id
            left join technologies_label on technologies_label.id = video_sessions_lectureslot.label_id
    ;
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