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
                'INSERT INTO course_module_completion_mapping (table_unique_key, '
                'course_id, course_name, topic_template_id, topic_pool_id, '
                'module_name, topics_with_topic_tree, topics_marked, '
                'module_completion_percent,module_running_status) '
                'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                'on conflict (table_unique_key) do update set course_name = EXCLUDED.course_name,'
                'topic_template_id = EXCLUDED.topic_template_id,'
                'topic_pool_id = EXCLUDED.topic_pool_id,'
                'module_name = EXCLUDED.module_name,'
                'topics_with_topic_tree = EXCLUDED.topics_with_topic_tree,'
                'topics_marked = EXCLUDED.topics_marked,'
                'module_completion_percent = EXCLUDED.module_completion_percent,'
                'module_running_status = EXCLUDED.module_running_status;',
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
    'course_module_completion_mapping_dag',
    default_args=default_args,
    description='course module completion mapping on the basis of topics marked',
    schedule_interval='0 22 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS course_module_completion_mapping (
            id serial,
            table_unique_key text not null PRIMARY KEY, 
            course_id int not null,
            course_name text,
            topic_template_id int,
            topic_pool_id int, 
            module_name text,
            topics_with_topic_tree int, 
            topics_marked int, 
            module_completion_percent real, 
            module_running_status text
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''with raw as 
    (with batch_tt as 
        -- batch wise topic lists in the topic tree
        (select 
            courses_course.id as course_id,
            courses_course.title as course_name,
            technologies_topicnode.id as topic_node_id,
            technologies_topic.id as topic_id,
            technologies_topicnode.created_at as topic_created_at,
            technologies_topic.title as topic_name,
            technologies_topictemplate.title as template_name
        from
            courses_course
        join technologies_topictemplate
            on technologies_topictemplate.id = courses_course.topic_template_id and courses_course.course_structure_id in (1,6,8,11,12,13,14,18,19,20,22,23,26,34)
                and lower(courses_course.unit_type) like 'learning'
        join technologies_topicnode
            on technologies_topicnode.topic_template_id = technologies_topictemplate.id
        join technologies_topic
            on technologies_topic.id = technologies_topicnode.topic_id
        group by 1,2,3,4,5,6,7),
    
    -- topic mapping with general module(s)
    topic_module_mapping as 
    
        (select
            technologies_topic.id as topic_id,
            technologies_topic.title as topic_name,
            technologies_topictemplate.id as topic_template_id, 
            technologies_topictemplate.title as module_name
        from
            technologies_topic
        join technologies_topicnode
            on technologies_topicnode.topic_id = technologies_topic.id
        join technologies_topictemplate
            on technologies_topictemplate.id = technologies_topicnode.topic_template_id 
                and technologies_topictemplate.id in (102,103,119,334,338,341,342,344,410))
                
    -- batch's topic tree topic(s) and module mapping    
    select 
        batch_tt.*,
        topic_module_mapping.topic_template_id /*as topic_topic_template_id,*/,
        video_sessions_lectureslottopicnodemapping.completed as topic_marked_status,
        topic_module_mapping.module_name,
        last_topic_marked_date
    from
        batch_tt
    left join topic_module_mapping
        on topic_module_mapping.topic_id = batch_tt.topic_id
    left join
                (with last_topic_marked as
                    (select 
                        course_id,
                        topic_template_id,
                        module_name,
                        max(marked_at) as last_topic_marked_date
                    from
                        (SELECT
                            temp.end_timestamp as "Lecture_Date",
                            id as lecture_id,
                            topic_id,
                            topic_name,
                            topic_template_id,
                            module_name,
                            course_id,
                            marked_at,
                            COUNT(id) as "Total",
                            MAX(diff) as "Time to Mark"
                        FROM 
                            (select 
                                video_sessions_lecture.id,
                                video_sessions_lectureslot.end_timestamp,
                                courses_course.id as course_id,
                                technologies_topicnode.topic_id,
                                technologies_topic.title as topic_name,
                                technologies_topictemplate.id as topic_template_id,
                                technologies_topictemplate.title as module_name,
                                (old_value::jsonb -> '2' -> '3') as a,
                                max(action_time) as marked_at,
                                ((EXTRACT(epoch FROM (max(action_time) - video_sessions_lectureslot.end_timestamp))/60)::INT) as diff
                            from
                                histories_historyentry
                            join video_sessions_lectureslot
                                on video_sessions_lectureslot.id = histories_historyentry.object_id
                            JOIN courses_course
                                ON video_sessions_lectureslot.course_id = courses_course.id
                            JOIN video_sessions_lecture
                                ON video_sessions_lecture.id = video_sessions_lectureslot.lecture_id
                            join video_sessions_lectureslottopicnodemapping
                                on video_sessions_lectureslottopicnodemapping.lecture_slot_id = video_sessions_lectureslot.id
                            left join technologies_topicnode
                                on technologies_topicnode.id = video_sessions_lectureslottopicnodemapping.topic_node_id
                            left join technologies_topic
                                on technologies_topic.id = technologies_topicnode.topic_id
                            left join technologies_topicnode ttn2
                                on ttn2.topic_id = technologies_topic.id
                            left join technologies_topictemplate
                                on technologies_topictemplate.id = ttn2.topic_template_id and technologies_topictemplate.id in (102,103,119,334,336,338,339,340,341,342,344,410)
                        
                            where content_type_id = 233 
                                AND video_sessions_lectureslot.is_topic_tree_independent= 'False'
                                AND video_sessions_lectureslot.parent_lecture_slot_id IS NULL
                                and video_sessions_lectureslot.is_deleted = 'False' 
                                and courses_course.end_timestamp > now() 
                                and video_sessions_lectureslot.start_timestamp < now()
                                and courses_course.course_structure_id in (1,6,8,11,12,13,18,19,14,20,22,23,26,34) 
                                AND video_sessions_lectureslottopicnodemapping.completed = 'true' 
                                AND action_time IS NOT NULL 
                                AND video_sessions_lectureslot.end_timestamp IS NOT NULL
                            group by 1,2,3,4,5,6,7,8
                            order by 1) as temp
                        WHERE diff IS NOT NULL 
                        GROUP BY 1,2,3,4,5,6,7,8
                        ORDER BY "Lecture_Date" DESC) a 
                        group by 1,2,3
                        having module_name is not null)
                    
                select
                    courses_course.id as course_id,
                    courses_course.title as course_name,
                    last_topic_marked.topic_template_id,
                    courses_course.topic_template_id as course_topic_template_id,
                    module_name,
                    last_topic_marked_date
                from
                    courses_course
                join last_topic_marked
                    on last_topic_marked.course_id = courses_course.id
                left join technologies_topictemplate
                    on technologies_topictemplate.id = courses_course.topic_template_id
                left join technologies_topicnode
                    on technologies_topicnode.topic_template_id = technologies_topictemplate.id) last_topic_marked_at
                        on last_topic_marked_at.course_id = batch_tt.course_id and last_topic_marked_at.topic_template_id = topic_module_mapping.topic_template_id

    left join video_sessions_lectureslottopicnodemapping
        on video_sessions_lectureslottopicnodemapping.topic_node_id = batch_tt.topic_node_id)
    
    select
        concat(course_id, topic_template_id, course_id) as table_unique_key,
        course_id,
        course_name,
        topic_template_id,
        case
            when topic_template_id = 338 then 177
            when topic_template_id = 334 then 173
            when topic_template_id in (119, 342) then 150
            when topic_template_id = 102 then 145
            when topic_template_id = 103 then 146
            else 0
        end as topic_pool_id,
        module_name, 
        (count(distinct topic_id) - 1) as topics_with_topic_tree,
        count(distinct topic_id) filter (where topic_marked_status = true) as topics_marked,
        case
            when (count(distinct topic_id) - 1) <> 0 then count(distinct topic_id) filter (where topic_marked_status = true) * 100.0 / ((count(distinct topic_id) - 1))
            else null 
        end as module_completion_percent,
        case
            when (count(distinct topic_id) - 1) <> 0 and (count(distinct topic_id) filter (where topic_marked_status = true) * 100.0 / ((count(distinct topic_id) - 1))) >= 90
            then 'Module Completed'
            when count(distinct topic_id) filter (where topic_marked_status = true) > 0 then 'Module Started'
            when count(distinct topic_id) filter (where topic_marked_status = true) = 0 then 'Module Not Started'
        end as module_running_status
    from
        raw
    group by 1,2,3,4,5,6
    having module_name is not null
    order by 1 desc, 6, 7;
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