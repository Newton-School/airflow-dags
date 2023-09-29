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
            'INSERT INTO course_component_topic_module_mapping (table_unique_key,'
            'course_id,'
            'course_name,'
            'course_component_id,'
            'module_name,'
            'topic_template_id,'
            'module_topic_name,'
            'topic_topic_id,'
            'topic_topic_name,'
            'module_topic_deadline_timestamp,'
            'module_topic_unlock_timestamp,'
            'module_topic_clearance_points,'
            'module_deadline_timestamp,'
            'module_unlock_timestamp,'
            'module_clearance_points) '
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (table_unique_key) do update set course_name = EXCLUDED.course_name,'
            'module_name = EXCLUDED.module_name,'
            'module_topic_name = EXCLUDED.module_topic_name,'
            'topic_topic_name = EXCLUDED.topic_topic_name,'
            'module_topic_deadline_timestamp = EXCLUDED.module_topic_deadline_timestamp,'
            'module_topic_unlock_timestamp = EXCLUDED.module_topic_unlock_timestamp,'
            'module_topic_clearance_points = EXCLUDED.module_topic_clearance_points,'
            'module_deadline_timestamp = EXCLUDED.module_deadline_timestamp,'
            'module_unlock_timestamp = EXCLUDED.module_unlock_timestamp,'
            'module_clearance_points = EXCLUDED.module_clearance_points;',
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


dag = DAG(
    'course_component_topic_module_mapping_dag',
    default_args=default_args,
    description='Course component mapping at module and topic (module accdng to normal nomenclature) and with technologies_topics.id level',
    schedule_interval='30 20 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS course_component_topic_module_mapping (
            id serial,
            table_unique_key text not null PRIMARY KEY,
            course_id int, 
            course_name text,
            course_component_id int,
            module_name text,
            topic_template_id int,
            module_topic_name text,
            topic_topic_id int,
            topic_topic_name text,
            module_topic_deadline_timestamp timestamp, 
            module_topic_unlock_timestamp timestamp,
            module_topic_clearance_points int,
            module_deadline_timestamp timestamp,
            module_unlock_timestamp timestamp,
            module_clearance_points int
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''
        select 
            concat(courses_course.id,'_',cc2.object_id,'_',technologies_topic.id) as table_unique_key,
            courses_course.id as course_id,
            courses_course.title as course_name,
            courses_coursecomponent.id as course_component_id,
            courses_coursecomponent.title as module_name,
            cc2.object_id as topic_template_id,
            cc2.title as module_topic_name,
            technologies_topic.id as topic_topic_id,
            technologies_topic.title as topic_topic_name,
            cc2.deadline_timestamp as module_topic_deadline_timestamp,
            cc2.unlock_timestamp as module_topic_unlock_timestamp,
            cc2.clearance_points as module_topic_clearance_points,
            courses_coursecomponent.deadline_timestamp as module_deadline_timestamp,
            courses_coursecomponent.unlock_timestamp as module_unlock_timestamp,
            courses_coursecomponent.clearance_points as module_clearance_points
        from
            courses_coursecomponent
        join courses_course
            on courses_course.id = courses_coursecomponent.course_id
        join courses_coursecomponent cc2
            on cc2.parent_id = courses_coursecomponent.id
        join technologies_topicnode
            on technologies_topicnode.topic_template_id = cc2.object_id
        join technologies_topic
            on technologies_topic.id = technologies_topicnode.topic_id
        where courses_coursecomponent.parent_id is null
        and courses_coursecomponent.component_type = 2
        and courses_coursecomponent.unlock_timestamp <= current_date
        order by 3;
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