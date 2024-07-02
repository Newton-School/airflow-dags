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
                'INSERT INTO topic_node_utility_mapping (table_unique_key,'
                'lecture_id,'
                'course_id,'
                'lecture_slot_id,'
                'start_timestamp,'
                'end_timestamp,'
                'mandatory,'
                'child_video_session,'
                'is_topic_tree_independent,'
                'lecture_slot_is_deleted,'
                'topic_marked_completed,'
                'topic_node_id,'
                'activity_type,'
                'activity_sub_type,'
                'question_count)'
                'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                'on conflict (table_unique_key) do update set lecture_slot_id = EXCLUDED.lecture_slot_id,'
                'start_timestamp = EXCLUDED.start_timestamp,'
                'end_timestamp = EXCLUDED.end_timestamp,'
                'mandatory = EXCLUDED.mandatory,'
                'child_video_session = EXCLUDED.child_video_session,'
                'is_topic_tree_independent = EXCLUDED.is_topic_tree_independent,'
                'lecture_slot_is_deleted = EXCLUDED.lecture_slot_is_deleted,'
                'topic_marked_completed = EXCLUDED.topic_marked_completed,'
                'activity_type = EXCLUDED.activity_type,'
                'activity_sub_type = EXCLUDED.activity_sub_type,'
                'question_count = EXCLUDED.question_count;',
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
                )
        )
    pg_conn.commit()


def cleanup_assignment_question_mapping(**kwargs):
    pg_hook_read_replica = PostgresHook(postgres_conn_id='postgres_read_replica')  # Use the read replica connection
    pg_conn_read_replica = pg_hook_read_replica.get_conn()
    pg_cursor_read_replica = pg_conn_read_replica.cursor()

    pg_cursor_read_replica.execute('''
        select 
            concat(video_sessions_lecture.id,'_',video_sessions_lecture.course_id,'_',technologies_topicnodeutilitymapping.topic_node_id,'_',technologies_topicnodeutilitymapping.entity_type,'_',technologies_topicnodeutilitymapping.utility_type) as table_unique_key
        from
            video_sessions_lecture
        join video_sessions_lectureslot
            on video_sessions_lecture.id = video_sessions_lectureslot.lecture_id 
        left join video_sessions_lectureslottopicnodemapping
            on video_sessions_lectureslottopicnodemapping.lecture_slot_id = video_sessions_lectureslot.id 
        left join technologies_topicnodeutilitymapping
            on technologies_topicnodeutilitymapping.topic_node_id = video_sessions_lectureslottopicnodemapping.topic_node_id;
    ''')

    unique_keys = [row[0] for row in pg_cursor_read_replica.fetchall()]
    pg_conn_read_replica.close()

    pg_hook_result_db = PostgresHook(postgres_conn_id='postgres_result_db')
    pg_conn_result_db = pg_hook_result_db.get_conn()
    pg_cursor_result_db = pg_conn_result_db.cursor()

    pg_cursor_result_db.execute(f'''
        DELETE FROM topic_node_utility_mapping
        WHERE table_unique_key NOT IN ({','.join(['%s'] * len(unique_keys))})
    ''', unique_keys)
    pg_conn_result_db.commit()


dag = DAG(
        'topic_node_utility_mapping_DAG',
        default_args=default_args,
        description='topic node utility mapping for expected questions (assignments and assessments) per utility',
        schedule_interval='15 21 * * *',
        catchup=False
)

create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres_result_db',
        sql='''CREATE TABLE IF NOT EXISTS topic_node_utility_mapping (
            id serial not null,
            table_unique_key text not null PRIMARY KEY,
            lecture_id bigint,
            course_id int,
            lecture_slot_id bigint,
            start_timestamp timestamp,
            end_timestamp timestamp,
            mandatory boolean,
            child_video_session boolean,
            is_topic_tree_independent boolean,
            lecture_slot_is_deleted boolean,
            topic_marked_completed boolean,
            topic_node_id bigint,
            activity_type text,
            activity_sub_type text, 
            question_count int
        );
    ''',
        dag=dag
)
transform_data = PostgresOperator(
        task_id='transform_data',
        postgres_conn_id='postgres_read_replica',
        sql='''
        select 
            concat(video_sessions_lecture.id,'_',video_sessions_lecture.course_id,'_',technologies_topicnodeutilitymapping.topic_node_id,'_',technologies_topicnodeutilitymapping.entity_type,'_',technologies_topicnodeutilitymapping.utility_type) as table_unique_key,
            video_sessions_lecture.id as lecture_id,
            video_sessions_lecture.course_id,
            video_sessions_lectureslot.id as lecture_slot_id,
            video_sessions_lecture.start_timestamp,
            video_sessions_lecture.end_timestamp,
            video_sessions_lecture.mandatory,
            video_sessions_lecture.child_video_session,
            video_sessions_lectureslot.is_topic_tree_independent,
            video_sessions_lectureslot.is_deleted as lecture_slot_is_deleted,
            video_sessions_lectureslottopicnodemapping.completed as topic_marked_completed,
            technologies_topicnodeutilitymapping.topic_node_id,
            case
                when technologies_topicnodeutilitymapping.entity_type = 1 then 'Assignments'
                when technologies_topicnodeutilitymapping.entity_type = 2 then 'Assessments'
            end as activity_type,
        
            case
                when technologies_topicnodeutilitymapping.utility_type = 1 then 'In-Class'
                when technologies_topicnodeutilitymapping.utility_type = 2 then 'Post-Class'
                when technologies_topicnodeutilitymapping.utility_type = 3 then 'Module Contests'
            end as activity_sub_type,
            question_count
        from
            video_sessions_lecture
        join video_sessions_lectureslot
            on video_sessions_lecture.id = video_sessions_lectureslot.lecture_id 
        left join video_sessions_lectureslottopicnodemapping
            on video_sessions_lectureslottopicnodemapping.lecture_slot_id = video_sessions_lectureslot.id 
        left join technologies_topicnodeutilitymapping
            on technologies_topicnodeutilitymapping.topic_node_id = video_sessions_lectureslottopicnodemapping.topic_node_id
        order by 5 desc;
        ''',
        dag=dag
)
extract_python_data = PythonOperator(
        task_id='extract_python_data',
        python_callable=extract_data_to_nested,
        provide_context=True,
        dag=dag
)

cleanup_data = PythonOperator(
        task_id='cleanup_data',
        python_callable=cleanup_assignment_question_mapping,
        provide_context=True,
        dag=dag
)

create_table >> transform_data >> extract_python_data >> cleanup_data
