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
            'INSERT INTO group_sessions (table_unique_key,meeting_id,mentor_user_id,'
            'mentor_report_type,course_id,mentee_user_id,mentee_report_type,mentor_min_join_time,'
            'mentor_max_leave_time,mentor_total_time_in_mins,mentee_min_join_time,'
            'mentee_max_leave_time,mentee_total_time_in_mins,mentee_overlapping_time_in_mins)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (table_unique_key) do update set mentor_report_type = EXCLUDED.mentor_report_type,'
            'mentee_report_type = EXCLUDED.mentee_report_type,'
            'mentor_min_join_time = EXCLUDED.mentor_min_join_time,'
            'mentor_max_leave_time = EXCLUDED.mentor_max_leave_time,'
            'mentor_total_time_in_mins = EXCLUDED.mentor_total_time_in_mins,'
            'mentee_min_join_time = EXCLUDED.mentee_min_join_time,'
            'mentee_max_leave_time = EXCLUDED.mentee_max_leave_time,'
            'mentee_total_time_in_mins = EXCLUDED.mentee_total_time_in_mins'
            'mentee_overlapping_time_in_mins = EXCLUDED.mentee_overlapping_time_in_mins;',
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
    'group_sessions_dag',
    default_args=default_args,
    description='Group Sessions mentor and mentee data',
    schedule_interval='0 3 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS group_sessions (
            id serial not null,
            table_unique_key double precision not null PRIMARY KEY,
            meeting_id bigint,
            mentor_user_id bigint,
            mentor_report_type int,
            course_id int,
            mentee_user_id bigint,
            mentee_report_type int,
            mentor_min_join_time timestamp,
            mentor_max_leave_time timestamp,
            mentor_total_time_in_mins bigint,
            mentee_min_join_time bigint,
            mentee_max_leave_time bigint,
            mentee_total_time_in_mins bigint,
            mentee_overlapping_time_in_mins bigint
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''with mentor_data as

    (select 
        video_sessions_meeting.id as meeting_id,
        video_sessions_meeting.booked_by_id as mentor_user_id,
        video_sessions_meetingcourseuserreport.report_type,
        video_sessions_meeting.course_id,
        min(video_sessions_meetingcourseuserreport.created_at) as min_created_at,
        min(video_sessions_meetingcourseuserreport.join_time) as min_join_time,
        max(video_sessions_meetingcourseuserreport.leave_time) as max_leave_time,
        sum(duration) as total_time
    from
        video_sessions_meeting
    join courses_course
        on courses_course.id = video_sessions_meeting.course_id
    join courses_courseusermapping
        on courses_courseusermapping.course_id = courses_course.id and video_sessions_meeting.booked_by_id = courses_courseusermapping.user_id
        
    left join video_sessions_meetingcourseuserreport
        on video_sessions_meetingcourseuserreport.course_user_mapping_id = courses_courseusermapping.id and video_sessions_meetingcourseuserreport.meeting_id = video_sessions_meeting.id 
            and video_sessions_meetingcourseuserreport.report_type in (1,2,4)
    
    where report_type in (1,2,4)
    group by 1,2,3,4),

mentee_data as 

    (select 
        video_sessions_meeting.id as meeting_id,
        video_sessions_meeting_booked_with.user_id as mentee_user_id,
        video_sessions_meetingcourseuserreport.report_type,
        video_sessions_meeting.course_id,
        min(video_sessions_meetingcourseuserreport.created_at) as min_created_at,
        min(video_sessions_meetingcourseuserreport.join_time) as min_join_time,
        max(video_sessions_meetingcourseuserreport.leave_time) as max_leave_time,
        sum(duration) as total_time,
        
        sum(duration) 
            
            filter (where video_sessions_meetingcourseuserreport.report_type = mentor_data.report_type and (video_sessions_meetingcourseuserreport.join_time >= mentor_data.min_join_time)
                and (video_sessions_meetingcourseuserreport.leave_time <= mentor_data.max_leave_time)) as overlapping_time
    
    from
        video_sessions_meeting
    join courses_course
        on courses_course.id = video_sessions_meeting.course_id
    join video_sessions_meeting_booked_with
        on video_sessions_meeting_booked_with.meeting_id = video_sessions_meeting.id
    join courses_courseusermapping
        on courses_courseusermapping.course_id = courses_course.id and video_sessions_meeting_booked_with.user_id = courses_courseusermapping.user_id
        
    left join video_sessions_meetingcourseuserreport
        on video_sessions_meetingcourseuserreport.course_user_mapping_id = courses_courseusermapping.id and video_sessions_meetingcourseuserreport.meeting_id = video_sessions_meeting.id 
            and video_sessions_meetingcourseuserreport.report_type in (1,2,4)
    
    left join mentor_data
        on mentor_data.meeting_id = video_sessions_meeting.id and mentor_data.report_type = video_sessions_meetingcourseuserreport.report_type
    group by 1,2,3,4)
    
    
select
    cast(concat(mentor_data.meeting_id, row_number()over(order by mentor_data.meeting_id)) as double precision) as table_unique_key,
    mentor_data.meeting_id,
    mentor_user_id,
    mentor_data.report_type as mentor_report_type,
    mentor_data.course_id,
    mentee_data.mentee_user_id,
    mentee_data.report_type as mentee_report_type,
    mentor_data.min_join_time as mentor_min_join_time,
    mentor_data.max_leave_time as mentor_max_leave_time,
    mentor_data.total_time/60 as mentor_total_time_in_mins,
    mentee_data.min_join_time as mentee_min_join_time,
    mentee_data.max_leave_time as mentee_max_leave_time,
    mentee_data.total_time/60 as mentee_total_time_in_mins,
    mentee_data.overlapping_time/60 as mentee_overlapping_time_in_mins
    
from
    mentor_data
left join mentee_data
    on mentor_data.meeting_id = mentee_data.meeting_id and mentee_data.report_type = mentor_data.report_type
order by 7 desc;
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