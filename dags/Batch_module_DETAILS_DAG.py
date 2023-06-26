from airflow import DAG
# from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from datetime import datetime

from sqlalchemy_utils.types.enriched_datetime.pendulum_date import pendulum

default_args = {
    'owner': 'airflow',
    'max_active_tasks': 6,
    'max_active_runs': 6,
    'concurrency': 4,
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

            'INSERT INTO batch_module_details (table_unique_key,course_id,batch_name,'
            'template_id,template_name,first_lecture_date,start_week,last_lecture_date,'
            'end_week,number_of_running_week)'

            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'

            'on conflict (table_unique_key) do update set course_id = EXCLUDED.course_id,'
            'batch_name = EXCLUDED.batch_name,'
            'template_id = EXCLUDED.template_id,'
            'template_name = EXCLUDED.template_name,'
            'first_lecture_date = EXCLUDED.first_lecture_date,'
            'start_week = EXCLUDED.start_week,'
            'last_lecture_date = EXCLUDED.last_lecture_date,'
            'end_week = EXCLUDED.end_week,'
            'number_of_running_week = EXCLUDED.number_of_running_week;',
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
                transform_row[9]
            )
        )
    pg_conn.commit()


dag = DAG(
    'batch_module_details_dag',
    default_args=default_args,
    concurrency=4,
    max_active_tasks=6,
    max_active_runs=6,
    description='This table records first and last lecture date module wise in a batch',
    schedule_interval='30 20 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS batch_module_details (
            id serial,
            table_unique_key double precision not null PRIMARY KEY,
            course_id int,
            batch_name varchar(128),
            template_id int,
            template_name varchar(128),
            first_lecture_date date,
            start_week timestamp,
            last_lecture_date date,
            end_week timestamp,
            number_of_running_week int
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''with raw as 

    (with raw_data as 
    
        (select 
            courses_course.id as course_id,
            courses_course.title as batch_name,
            video_sessions_lecture.id as lecture_id,
            video_sessions_lecture.title as lecture_title,
            date(video_sessions_lecture.start_timestamp) as lecture_date,
            technologies_topic.title as topic_name,
            technologies_topictemplate.id as template_id,
            technologies_topictemplate.title as template_name
        from
            video_sessions_lecture
        join courses_course
            on courses_course.id = video_sessions_lecture.course_id
        left join video_sessions_lecturetopicmapping
            on video_sessions_lecturetopicmapping.lecture_id = video_sessions_lecture.id
        left join technologies_topic
            on technologies_topic.id = video_sessions_lecturetopicmapping.topic_id
        left join technologies_topicnode
            on technologies_topicnode.topic_id = technologies_topic.id
        left join technologies_topictemplate
            on technologies_topictemplate.id = technologies_topicnode.topic_template_id and technologies_topictemplate.id in 
            (102,103,119,334,336,338,339,340,341,342,344,410)
        group by 1,2,3,4,5,6,7,8
        having 
            technologies_topictemplate.title is not null
        order by 3 desc)
    
    
    select 
        course_id,
         batch_name,
         lecture_id,
         lecture_title,
         lecture_date,
         date_trunc('week', lecture_date) as week_view,
         template_id,
         template_name 
    from
        raw_data
    group by 1,2,3,4,5,6,7,8
    order by 3 desc, 1)
    
select
    concat(course_id, template_id) as table_unique_key,
    course_id,
    batch_name,
    template_id,
    template_name,
    min(lecture_date) as first_lecture_date,
    min(week_view) as start_week,
    max(lecture_date) as last_lecture_date,
    max(week_view) as end_week,
    extract('day' from (max(week_view) - min(week_view)))/7 as number_of_running_week
from
    raw
group by 1,2,3,4,5
order by 1 desc, 3,5;
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