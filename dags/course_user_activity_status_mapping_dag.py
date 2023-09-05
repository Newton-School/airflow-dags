from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime


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
            'INSERT INTO user_activity_status_mapping (user_id, student_name,'
            'lead_type, latest_activity_date, activity_status_7_days, activity_status_14_days, activity_status_30_days,'
            'last_activity, last_lecture_attended_on, last_question_attempted_on, last_quiz_attempted_on, '
            'last_mock_date, last_one_on_one_date, last_recorded_lecture_watched_on, last_group_session_date)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (user_id) do update set student_name = EXCLUDED.student_name,'
            'lead_type = EXCLUDED.lead_type,'
            'latest_activity_date = EXCLUDED.latest_activity_date,'
            'activity_status_7_days = EXCLUDED.activity_status_7_days,'
            'activity_status_14_days = EXCLUDED.activity_status_14_days,'
            'activity_status_30_days = EXCLUDED.activity_status_30_days,'
            'last_activity = EXCLUDED.last_activity,'
            'last_lecture_attended_on = EXCLUDED.last_lecture_attended_on,'
            'last_question_attempted_on = EXCLUDED.last_question_attempted_on,'
            'last_quiz_attempted_on = EXCLUDED.last_quiz_attempted_on,'
            'last_mock_date = EXCLUDED.last_mock_date,'
            'last_one_on_one_date = EXCLUDED.last_one_on_one_date,'
            'last_recorded_lecture_watched_on = EXCLUDED.last_recorded_lecture_watched_on,'
            'last_group_session_date = EXCLUDED.last_group_session_date',
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
    'user_activity_status_mapping_dag',
    default_args=default_args,
    concurrency=4,
    max_active_tasks=6,
    max_active_runs=6,
    description='assigns activity status as active/ inactive to each user_id irrespective of user course',
    schedule_interval='30 0 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS user_activity_status_mapping (
            id serial,
            user_id bigint not null PRIMARY KEY,
            student_name text,
            lead_type text,
            latest_activity_date timestamp, 
            activity_status_7_days text,
            activity_status_14_days text,
            activity_status_30_days text, 
            last_activity text,
            last_lecture_attended_on date,
            last_question_attempted_on timestamp, 
            last_quiz_attempted_on timestamp, 
            last_mock_date timestamp,
            last_one_on_one_date timestamp, 
            last_recorded_lecture_watched_on date,
            last_group_session_date timestamp
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_result_db',
    sql='''
        with live_lectures as 
                        (select  
                            user_id,
                            max(date(l.start_timestamp)) filter (where lecture_attendance_status like 'Present') as last_lecture_attended_on
                        from
                            (with raw_data as 
                                (select 
                                    course_user_mapping_id,
                                    lecture_id,
                                    join_time,
                                    leave_time,
                                    extract('epoch' from leave_time - join_time) as time_spent_in_seconds,
                                    user_type,
                                    overlapping_time_seconds,
                                    overlapping_time_minutes
                                from
                                    lecture_engagement_time
                                group by 1,2,3,4,5,6,7,8),
                                
                            instructor_data as 
                                (select 
                                    raw_data.lecture_id,
                                    raw_data.course_user_mapping_id,
                                    user_id,
                                    sum(time_spent_in_seconds) / 60 as total_inst_time_in_mins
                                from
                                    raw_data
                                join course_user_mapping
                                    on course_user_mapping.course_user_mapping_id = raw_data.course_user_mapping_id
                                where user_type like 'Instructor'
                                group by 1,2,3),
                            
                            users_data as 
                                (select 
                                    raw_data.lecture_id,
                                    raw_data.course_user_mapping_id,
                                    course_user_mapping.user_id,
                                    instructor_data.total_inst_time_in_mins,
                                    sum(time_spent_in_seconds) / 60 as total_user_time_in_mins,
                                    sum(overlapping_time_minutes) as total_overlap_time_in_mins,
                                    sum(overlapping_time_seconds) as total_overlap_time_in_seconds
                                from
                                    raw_data
                                join course_user_mapping
                                    on course_user_mapping.course_user_mapping_id = raw_data.course_user_mapping_id
                                left join instructor_data
                                    on instructor_data.lecture_id = raw_data.lecture_id
                                where user_type like 'User'
                                group by 1,2,3,4)
                                
                            select 
                                *,
                                case
                                    when (users_data.total_overlap_time_in_mins / users_data.total_inst_time_in_mins) >= 0.1 then 'Present'
                                    else 'Insufficient Overlap Time - Absent'
                                end as lecture_attendance_status
                            from
                                users_data
                            order by 1 desc, 2,3) a
                        join lectures l
                            on l.lecture_id = a.lecture_id
                        group by 1
                        order by 2),
                        
        assignments as 
                        (select 
                            ui.user_id,
                            max(aqum.question_started_at) as last_question_attempted_on
                        from
                            users_info ui
                        left join assignment_question_user_mapping_new aqum 
                            on ui.user_id = aqum.user_id
                        group by 1),
                    
        assessments as 
                        (select 
                            ui.user_id,
                            max(aqum.option_marked_at) as last_question_attempted_on
                        from
                            users_info ui
                        left join assessment_question_user_mapping aqum 
                            on aqum.user_id = ui.user_id
                        group by 1),
                    
        one_to_one_cte as 
                        (select 
                            ui.user_id,
                            max(vscur.join_time) filter (where vscur.overlapping_time_minutes >= 1) as last_one_to_one_attended_on
                        from
                            users_info ui
                        left join video_sessions_one_to_one_course_user_reports vscur 
                            on vscur.user_id = ui.user_id
                        where one_to_one_type <> 7
                        group by 1),
                        
        mentor_session as 
                        (select 
                            ui.user_id,
                            max(vscur.join_time) filter (where vscur.overlapping_time_minutes >= 1) as last_one_to_one_attended_on
                        from
                            users_info ui
                        left join video_sessions_one_to_one_course_user_reports vscur 
                            on vscur.user_id = ui.user_id
                        where one_to_one_type = 7
                        group by 1),
                    
        recorded_lectures as 
                        (select 
                            ui.user_id,
                            max(rlcur.lecture_watch_date) filter (where rlcur.total_time_watched_in_mins>= 1)  as last_lecture_watch_on
                        from
                            users_info ui
                        join course_user_mapping cum 
                            on cum.user_id = ui.user_id 
                        left join recorded_lectures_course_user_reports rlcur 
                            on rlcur.course_user_mapping_id = cum.course_user_mapping_id
                        group by 1),
                        
        group_sessions as 
                        (select 
                            cum.user_id,
                            max(group_session_course_user_reports.join_time) filter (where group_session_course_user_reports.overlapping_time_minutes >= 1)  as last_gs_date
                        from
                            users_info ui	
                        join course_user_mapping cum 
                            on cum.user_id = ui.user_id 
                        left join group_session_course_user_reports
                            on group_session_course_user_reports.course_user_mapping_id = cum.course_user_mapping_id
                        group by 1)
                        
        select
            ui.user_id,
            concat(ui.first_name,' ', ui.last_name) as student_name,
            ui.lead_type,                
            greatest
                (live_lectures.last_lecture_attended_on, assignments.last_question_attempted_on, assessments.last_question_attempted_on,
                one_to_one_cte.last_one_to_one_attended_on, mentor_session.last_one_to_one_attended_on, recorded_lectures.last_lecture_watch_on,
                group_sessions.last_gs_date) as latest_activity_date,
            case 
                when date(greatest
                            (live_lectures.last_lecture_attended_on,assignments.last_question_attempted_on,assessments.last_question_attempted_on,
                            one_to_one_cte.last_one_to_one_attended_on, mentor_session.last_one_to_one_attended_on, recorded_lectures.last_lecture_watch_on,
                            group_sessions.last_gs_date)) - date((current_date - interval '7 days')) >= 0 then 'Active'
                else 'Inactive'
            end as activity_status_7_days,
            case 
                when date(greatest
                            (live_lectures.last_lecture_attended_on,assignments.last_question_attempted_on,assessments.last_question_attempted_on,
                            one_to_one_cte.last_one_to_one_attended_on, mentor_session.last_one_to_one_attended_on, recorded_lectures.last_lecture_watch_on,
                            group_sessions.last_gs_date)) - date((current_date - interval '14 days')) >= 0 then 'Active'
                else 'Inactive'
            end as activity_status_14_days,
            case 
                when date(greatest
                            (live_lectures.last_lecture_attended_on,assignments.last_question_attempted_on,assessments.last_question_attempted_on,
                            one_to_one_cte.last_one_to_one_attended_on, mentor_session.last_one_to_one_attended_on, recorded_lectures.last_lecture_watch_on,
                            group_sessions.last_gs_date)) - date((current_date - interval '30 days')) >= 0 then 'Active'
                else 'Inactive'
            end as activity_status_30_days,
            case
                when greatest
                    (live_lectures.last_lecture_attended_on,assignments.last_question_attempted_on,assessments.last_question_attempted_on,
                    one_to_one_cte.last_one_to_one_attended_on, mentor_session.last_one_to_one_attended_on, recorded_lectures.last_lecture_watch_on,
                    group_sessions.last_gs_date) = live_lectures.last_lecture_attended_on then 'Live Lecture'
                when greatest
                    (live_lectures.last_lecture_attended_on,assignments.last_question_attempted_on,assessments.last_question_attempted_on,
                    one_to_one_cte.last_one_to_one_attended_on, mentor_session.last_one_to_one_attended_on, recorded_lectures.last_lecture_watch_on,
                    group_sessions.last_gs_date) = assignments.last_question_attempted_on then 'Coding Question'
                when greatest
                    (live_lectures.last_lecture_attended_on,assignments.last_question_attempted_on,assessments.last_question_attempted_on,
                    one_to_one_cte.last_one_to_one_attended_on, mentor_session.last_one_to_one_attended_on, recorded_lectures.last_lecture_watch_on,
                    group_sessions.last_gs_date) = assessments.last_question_attempted_on then 'MCQ Question'
                when greatest
                    (live_lectures.last_lecture_attended_on,assignments.last_question_attempted_on,assessments.last_question_attempted_on,
                    one_to_one_cte.last_one_to_one_attended_on, mentor_session.last_one_to_one_attended_on, recorded_lectures.last_lecture_watch_on,
                    group_sessions.last_gs_date) = one_to_one_cte.last_one_to_one_attended_on then 'Mock Interview'
                when greatest
                    (live_lectures.last_lecture_attended_on,assignments.last_question_attempted_on,assessments.last_question_attempted_on,
                    one_to_one_cte.last_one_to_one_attended_on, mentor_session.last_one_to_one_attended_on, recorded_lectures.last_lecture_watch_on,
                    group_sessions.last_gs_date) = mentor_session.last_one_to_one_attended_on then 'One-on-One'
                when greatest
                    (live_lectures.last_lecture_attended_on,assignments.last_question_attempted_on,assessments.last_question_attempted_on,
                    one_to_one_cte.last_one_to_one_attended_on, mentor_session.last_one_to_one_attended_on, recorded_lectures.last_lecture_watch_on,
                    group_sessions.last_gs_date) = recorded_lectures.last_lecture_watch_on then 'Recorded Lecture'
                when greatest
                    (live_lectures.last_lecture_attended_on,assignments.last_question_attempted_on,assessments.last_question_attempted_on,
                    one_to_one_cte.last_one_to_one_attended_on, mentor_session.last_one_to_one_attended_on, recorded_lectures.last_lecture_watch_on,
                    group_sessions.last_gs_date) = group_sessions.last_gs_date then 'Group Session'
                else 'No Activity'
            end as last_activity,
            live_lectures.last_lecture_attended_on,
            assignments.last_question_attempted_on,
            assessments.last_question_attempted_on as last_quiz_attempted_on,
            one_to_one_cte.last_one_to_one_attended_on as last_mock_date,
            mentor_session.last_one_to_one_attended_on as last_one_on_one_date,
            recorded_lectures.last_lecture_watch_on as last_recorded_lecture_watched_on,
            group_sessions.last_gs_date as last_group_session_date
        from
            users_info ui
        left join live_lectures
            on live_lectures.user_id = ui.user_id 
        left join assignments
            on assignments.user_id = ui.user_id 
        left join assessments
            on assessments.user_id = ui.user_id 
        left join one_to_one_cte
            on one_to_one_cte.user_id = ui.user_id 
        left join group_sessions
            on group_sessions.user_id = ui.user_id 
        left join recorded_lectures
            on recorded_lectures.user_id = ui.user_id
        left join mentor_session
            on ui.user_id = mentor_session.user_id
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15;
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