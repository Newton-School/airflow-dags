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
            'INSERT INTO arl_lectures_x_users (table_unique_key, user_id, course_id,'
            'lecture_id, lecture_title, inst_user_id, template_name,'
            'lecture_date, inst_total_time, total_overlapping_time,'
            'overall_lectures_watched, live_lectures_attended, recorded_lectures_watched, mandatory)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (table_unique_key) do update set lecture_title = EXCLUDED.lecture_title,'
            'inst_user_id = EXCLUDED.inst_user_id,'
            'template_name=EXCLUDED.template_name,'
            'lecture_date=EXCLUDED.lecture_date,'
            'inst_total_time=EXCLUDED.inst_total_time,'
            'total_overlapping_time=EXCLUDED.total_overlapping_time,'
            'overall_lectures_watched=EXCLUDED.overall_lectures_watched,'
            'live_lectures_attended = EXCLUDED.live_lectures_attended,'
            'recorded_lectures_watched = EXCLUDED.recorded_lectures_watched,'
            'mandatory = EXCLUDED.mandatory;',
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
                transform_row[13]
            )
        )
    pg_conn.commit()


dag = DAG(
    'ARL_Lectures_x_Users',
    default_args=default_args,
    concurrency=4,
    max_active_tasks=6,
    max_active_runs=6,
    description='An Analytics Reporting Layer DAG for Lectures x user level',
    schedule_interval='45 1 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS arl_lectures_x_users (
            id serial,
            table_unique_key double precision not null PRIMARY KEY,
            user_id bigint,
            course_id int,
            lecture_id bigint,
            lecture_title varchar(1028),
            inst_user_id bigint,
            template_name varchar(256),
            lecture_date DATE,
            inst_total_time real,
            total_overlapping_time real,
            overall_lectures_watched int,
            live_lectures_attended int,
            recorded_lectures_watched int,
            mandatory boolean
            
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_result_db',
    sql='''with user_details as 
                (select 
                    wud.user_id,
                    wud.course_user_mapping_id,
                    c.course_id,
                    t.template_name,
                    t.topic_template_id,
                    l.lecture_id,
                    l.lecture_title,
                    l.mandatory,
                    date(l.start_timestamp) as lecture_date,
                    count(distinct l.lecture_id) filter (where lectures_info.lecture_id is not null) as overall_lectures_watched,
                    count(distinct l.lecture_id) filter (where llcur2.lecture_id is not null) as live_lectures_attended,
                    count(distinct l.lecture_id) filter (where rlcur2.lecture_id is not null) as recorded_lectures_watched,
                    sum(distinct llcur2.overlapping_time_in_mins) filter (where llcur2.lecture_id is not null and report_type = 4) as total_overlapping_time
                from
                    lectures l 
                join courses c 
                    on c.course_id = l.course_id and c.course_structure_id  in (1,6,8,11,12,13,14,18,19,20,22,23,26)
                left join weekly_user_details wud 
                	on wud.course_id = c.course_id and date(wud.week_view) = date(date_trunc('week',l.start_timestamp)) 
                		and wud.status in (5,8,9) and wud.label_mapping_id is null
                left join live_lectures_course_user_reports llcur2 
                    on llcur2.lecture_id = l.lecture_id and wud.course_user_mapping_id = llcur2.course_user_mapping_id 
                left join recorded_lectures_course_user_reports rlcur2 
                	on rlcur2.lecture_id = l.lecture_id and wud.course_user_mapping_id = rlcur2.course_user_mapping_id 
                left join 
                    (select 
                        lecture_id,
                        course_user_mapping_id,
                        overlapping_time_in_mins,
                        'live' as lecture_type
                    from 
                        live_lectures_course_user_reports llcur 
                    union 
                    select 
                        lecture_id,
                        course_user_mapping_id,
                        total_time_watched_in_mins,
                        'recorded' as lecture_type
                    from
                        recorded_lectures_course_user_reports rlcur) as lectures_info
                        on lectures_info.lecture_id = l.lecture_id and wud.course_user_mapping_id = lectures_info.course_user_mapping_id
                 left join lecture_topic_mapping ltm 
                    on ltm.lecture_id = llcur2.lecture_id and ltm.completed = true
                 left join topics t 
                    on t.topic_id = ltm.topic_id and t.topic_template_id in (102,103,119,334,336,338,339,340,341,342,344,410)
                
                group by 1,2,3,4,5,6,7,8,9),
            
            inst_details as 
                (select
                    c.course_id,
                    t.template_name,
                    l.lecture_id,
                    l.lecture_title,
                    l.mandatory,
                    cum.user_id as inst_user_id,
                    lim.inst_course_user_mapping_id,
                    date(l.start_timestamp) as lecture_date,
                    floor(sum(distinct llcur2.inst_total_time_in_mins))  as inst_total_time
                from
                    lectures l 
                join courses c 
                    on c.course_id = l.course_id and c.course_structure_id  in (1,6,8,11,12,13,14,18,19,20,22,23,26)
                left join live_lectures_course_user_reports llcur2 
                    on llcur2.lecture_id = l.lecture_id
                left join lecture_topic_mapping ltm 
                    on ltm.lecture_id = llcur2.lecture_id and ltm.completed = true
                left join topics t 
                    on t.topic_id = ltm.topic_id and t.topic_template_id in (102,103,119,334,336,338,339,340,341,342,344,410)
                left join lecture_instructor_mapping lim 
                    on lim.lecture_id = l.lecture_id 
                left join course_user_mapping cum 
                    on cum.course_user_mapping_id = lim.inst_course_user_mapping_id and c.course_id = cum.course_id 
                group by 1,2,3,4,5,6,7,8)
                
            select 
                distinct concat(user_details.user_id,user_details.lecture_id,user_details.topic_template_id,user_details.course_id,inst_details.inst_user_id/*,extract(day from user_details.lecture_date),extract(month from user_details.lecture_date),extract(year from user_details.lecture_date)*/) as table_unique_key,
                user_details.user_id,
                user_details.course_id,
                user_details.lecture_id,
                user_details.lecture_title,
                inst_details.inst_user_id,
                user_details.template_name,
                user_details.lecture_date,
                inst_details.inst_total_time,
                user_details.total_overlapping_time,
                user_details.overall_lectures_watched,
                user_details.live_lectures_attended,
                user_details.recorded_lectures_watched,
                user_details.mandatory
            from
                user_details
            left join inst_details
            	on user_details.lecture_id = inst_details.lecture_id;
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