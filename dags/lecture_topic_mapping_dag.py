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
                'INSERT INTO lecture_topic_mapping (lecture_topic_mapping_id,completed,lecture_slot_id,lecture_id,topic_node_id,created_at,topic_id) '
                'VALUES (%s,%s,%s,%s,%s,%s,%s)'
                'on conflict (lecture_topic_mapping_id) do update set completed = EXCLUDED.completed,lecture_slot_id= EXCLUDED.lecture_slot_id,'
                'lecture_id=EXCLUDED.lecture_id,topic_node_id=EXCLUDED.topic_node_id, topic_id=EXCLUDED.topic_id ;',
                (
                    transform_row[0],
                    transform_row[1],
                    transform_row[2],
                    transform_row[3],
                    transform_row[4],
                    transform_row[5],
                    transform_row[6],
                 )
        )
    pg_conn.commit()


dag = DAG(
    'Lecture_topic_mapping_DAG',
    default_args=default_args,
    description='Lecture Topic Mapping Table DAG',
    schedule_interval='0 20 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS lecture_topic_mapping (
            lecture_topic_mapping_id bigint not null PRIMARY KEY,
            completed boolean,
            lecture_slot_id bigint,
            lecture_id bigint,
            topic_node_id bigint,
            created_at timestamp,
            topic_id bigint
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''select
            video_sessions_lectureslottopicnodemapping.id as lecture_topic_mapping_id,
            video_sessions_lectureslottopicnodemapping.completed,
            video_sessions_lectureslottopicnodemapping.lecture_slot_id,
            video_sessions_lectureslot.lecture_id,
            video_sessions_lectureslottopicnodemapping.topic_node_id,
            cast(video_sessions_lectureslottopicnodemapping.created_at as varchar) as created_at,
            technologies_topicnode.topic_id
            from video_sessions_lectureslottopicnodemapping
            left join technologies_topicnode on technologies_topicnode.id = video_sessions_lectureslottopicnodemapping.topic_node_id
            left join video_sessions_lectureslot on video_sessions_lectureslot.id = video_sessions_lectureslottopicnodemapping.lecture_slot_id
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