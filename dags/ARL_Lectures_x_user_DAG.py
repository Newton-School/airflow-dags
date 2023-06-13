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
            'INSERT INTO arl_lectures_x_users (table_unique_key,user_id,course_id,inst_user_id,template_name,'
            'lecture_date,'
            'total_lectures,overall_lectures_watched,live_lectures_attended,inst_total_time,total_overlapping_time)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (table_unique_key) do update set template_name=EXCLUDED.template_name,'
            'lecture_date=EXCLUDED.lecture_date,'
            'total_lectures=EXCLUDED.total_lectures,overall_lectures_watched=EXCLUDED.overall_lectures_watched,'
            'live_lectures_attended=EXCLUDED.live_lectures_attended,inst_total_time=EXCLUDED.inst_total_time,'
            'total_overlapping_time=EXCLUDED.total_overlapping_time;',
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
            )
        )
    pg_conn.commit()


dag = DAG(
    'ARL_Lectures_x_Users',
    default_args=default_args,
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
            inst_user_id bigint,
            template_name varchar(256),
            lecture_date DATE,
            total_lectures int,
            overall_lectures_watched int,
            live_lectures_attended int,
            inst_total_time real,
            total_overlapping_time real
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_result_db',
    sql='''with user_details_raw as 
                (select 
                    cum.user_id,
                    cum.course_user_mapping_id,
                    c.course_id,
                    t.template_name,
                    t.topic_template_id,
                    l.lecture_id,
                    date(l.start_timestamp) as lecture_date,
                    count(distinct l.lecture_id) as total_lectures,
                    count(distinct l.lecture_id) filter (where lectures_info.lecture_id is not null) as overall_lectures_watched,
                    count(distinct l.lecture_id) filter (where llcur2.lecture_id is not null) as live_lectures_attended,
                    sum(distinct llcur2.overlapping_time_in_mins) filter (where llcur2.lecture_id is not null and report_type = 4) as total_overlapping_time
                from
                    lectures l 
                join courses c 
                    on c.course_id = l.course_id and c.course_structure_id  in (1,6,8,11,12,13,14,18,19,20,22,23,26)
                        and l.mandatory = true
                join course_user_mapping cum 
                    on cum.course_id = c.course_id and cum.status in (5,8,9) and cum.label_id is null
                left join live_lectures_course_user_reports llcur2 
                    on llcur2.lecture_id = l.lecture_id and cum.course_user_mapping_id = llcur2.course_user_mapping_id 
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
                        on lectures_info.lecture_id = l.lecture_id and cum.course_user_mapping_id = lectures_info.course_user_mapping_id
                 join lecture_topic_mapping ltm 
                    on ltm.lecture_id = llcur2.lecture_id and ltm.completed = true
                 join topics t 
                    on t.topic_id = ltm.topic_id and t.topic_template_id in (102,103,119,334,336,338,339,340,341,342,344,410)
                
                group by 1,2,3,4,5,6,7),
                
            user_details as 
                (select 
                    user_id,
                    course_id,
                    topic_template_id,
                    template_name,
                    lecture_date,
                    sum(total_lectures) as lectures_conducted,
                    sum(overall_lectures_watched) as overall_lectures_watched,
                    sum(live_lectures_attended) as live_lectures_attended,
                    sum(total_overlapping_time) as total_overlapping_time
                from
                    user_details_raw
                group by 1,2,3,4,5),
            
            inst_details as 
                (select
                    c.course_id,
                    t.template_name,
                    cum.user_id as inst_user_id,
                    lim.inst_course_user_mapping_id,
                    date(l.start_timestamp) as lecture_date,
                    count(distinct l.lecture_id) as total_lectures,
                    floor(sum(distinct llcur2.inst_total_time_in_mins))  as inst_total_time
                from
                    lectures l 
                join courses c 
                    on c.course_id = l.course_id and c.course_structure_id  in (1,6,8,11,12,13,14,18,19,20,22,23,26)
                        and l.mandatory = true
                join live_lectures_course_user_reports llcur2 
                    on llcur2.lecture_id = l.lecture_id
                join lecture_topic_mapping ltm 
                    on ltm.lecture_id = llcur2.lecture_id and ltm.completed = true
                join topics t 
                    on t.topic_id = ltm.topic_id and t.topic_template_id in (102,103,119,334,336,338,339,340,341,342,344,410)
                join lecture_instructor_mapping lim 
                    on lim.lecture_id = l.lecture_id 
                join course_user_mapping cum 
                    on cum.course_user_mapping_id = lim.inst_course_user_mapping_id and c.course_id = cum.course_id 
                group by 1,2,3,4,5)
            select 
                distinct concat(user_details.user_id,user_details.topic_template_id,user_details.course_id,inst_details.inst_user_id,extract(day from user_details.lecture_date),extract(month from user_details.lecture_date),extract(year from user_details.lecture_date)) as table_unique_key,
                user_details.user_id,
                user_details.course_id,
                inst_details.inst_user_id,
                user_details.template_name,
                user_details.lecture_date,
                inst_details.total_lectures,
                user_details.overall_lectures_watched,
                user_details.live_lectures_attended,
                inst_details.inst_total_time,
                user_details.total_overlapping_time
            from
                user_details
            join inst_details
                on inst_details.course_id = user_details.course_id and user_details.template_name = inst_details.template_name 
                and user_details.lecture_date = inst_details.lecture_date;
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