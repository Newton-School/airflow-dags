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
            'INSERT INTO course_user_activity_status_mapping (table_unique_key, user_id, student_name,'
            'course_id, course_name, course_structure_class, lead_type, label_mapping_status, student_category,'
            'latest_activity_date, activity_status_7_days, activity_status_14_days, activity_status_30_days,'
            'last_activity, last_lecture_attended_on, last_question_attempted_on, last_quiz_attempted_on, '
            'last_mock_date, last_one_to_one_date, last_recorded_lecture_watched_on, last_group_session_date)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (table_unique_key) do update set student_name = EXCLUDED.student_name,'
            'course_name = EXCLUDED.course_name,'
            'course_structure_class = EXCLUDED.course_structure_class,'
            'lead_type = EXCLUDED.lead_type,'
            'label_mapping_status = EXCLUDED.label_mapping_status,'
            'student_category = EXCLUDED.student_category,'
            'last_activity_date = EXCLUDED.last_activity_date,'
            'activity_status_7_days = EXCLUDED.activity_status_7_days,'
            'activity_status_14_days = EXCLUDED.activity_status_14_days,'
            'activity_status_30_days = EXCLUDED.activity_status_30_days,'
            'last_activity = EXCLUDED.last_activity,'
            'last_lecture_attended_on = EXCLUDED.last_lecture_attended_on,'
            'last_question_attempted_on = EXCLUDED.last_question_attempted_on,'
            'last_quiz_attempted_on = EXCLUDED.last_quiz_attempted_on,'
            'last_mock_date = EXCLUDED.last_mock_date,'
            'last_one_to_one_date = EXCLUDED.last_one_to_one_date,'
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
                transform_row[15],
                transform_row[16],
                transform_row[17],
                transform_row[18],
                transform_row[19],
                transform_row[20],
            )
        )
    pg_conn.commit()


dag = DAG(
    'course_user_activity_status_mapping',
    default_args=default_args,
    concurrency=4,
    max_active_tasks=6,
    max_active_runs=6,
    description='assigns activity status as active/ inactive to each course_user_mapping_id with cum.status in (8,9,11,12,30)',
    schedule_interval='0 2 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS course_user_activity_status_mapping (
            id serial,
            table_unique_key text not null PRIMARY KEY,
            user_id bigint,
            student_name text,
            course_id int,
            course_name text,
            course_structure_class text,
            lead_type text,
            label_mapping_status text, 
            student_category text, 
            latest_activity_date timestamp, 
            activity_status_7_days text,
            activity_status_14_days text,
            activity_status_30_days text, 
            last_activity text,
            last_lecture_attended_on timestamp,
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
                    cum.user_id,
                    concat(ui.first_name,' ', ui.last_name) as student_name,
                    c.course_id,
                    cum.course_user_mapping_id,
                    max(let.join_time) filter (where let.overlapping_time_minutes >= 1) as last_lecture_attended_on
                from
                    users_info ui	
                join course_user_mapping cum 
                    on cum.user_id = ui.user_id 
                join courses c 
                    on c.course_id = cum.course_id and c.course_structure_id in (1,6,8,11,12,13,14,18,19,20,22,23,26,34)
                        and cum.status in (8,9,11,12,30) and c.course_id > 200
                left join lectures l
                    on l.course_id = c.course_id 
                left join lecture_engagement_time let
                    on let.lecture_id = l.lecture_id and cum.course_user_mapping_id = let.course_user_mapping_id
                group by 1,2,3,4),
                
            assignments as 
                (select 
                    cum.user_id,
                    concat(ui.first_name,' ', ui.last_name) as student_name,
                    c.course_id,
                    cum.course_user_mapping_id,
                    max(aqum.question_started_at) as last_question_attempted_on
                from
                    users_info ui
                join course_user_mapping cum 
                    on cum.user_id = ui.user_id 
                join courses c 
                    on c.course_id = cum.course_id and c.course_structure_id in (1,6,8,11,12,13,14,18,19,20,22,23,26,34)
                        and cum.status in (8,9,11,12,30) and c.course_id > 200
                left join assignments a 
                    on a.course_id = c.course_id and a.hidden = false
                left join assignment_question_user_mapping aqum 
                    on aqum.user_id = cum.user_id and a.assignment_id = aqum.assignment_id
                group by 1,2,3,4),
            
            assessments as 
                (select 
                    cum.user_id,
                    concat(ui.first_name,' ', ui.last_name) as student_name,
                    c.course_id,
                    cum.course_user_mapping_id,
                    max(aqum.option_marked_at) as last_question_attempted_on
                from
                    users_info ui
                join course_user_mapping cum 
                    on cum.user_id = ui.user_id 
                join courses c 
                    on c.course_id = cum.course_id and c.course_structure_id in (1,6,8,11,12,13,14,18,19,20,22,23,26,34)
                        and cum.status in (8,9,11,12,30) and c.course_id > 200
                left join assessments a 
                    on a.course_id = c.course_id and a.hidden = false
                left join assessment_question_user_mapping aqum 
                    on aqum.user_id = cum.user_id and a.assessment_id = aqum.assessment_id
                group by 1,2,3,4),
            
            one_to_one_cte as 
                (select 
                    cum.user_id,
                    concat(ui.first_name,' ', ui.last_name) as student_name,
                    c.course_id,
                    cum.course_user_mapping_id,
                    max(vscur.join_time) filter (where vscur.overlapping_time_minutes >= 1) as last_one_to_one_attended_on
                from
                    users_info ui
                join course_user_mapping cum 
                    on cum.user_id = ui.user_id 
                join courses c 
                    on c.course_id = cum.course_id  and c.course_structure_id in (1,6,8,11,12,13,14,18,19,20,22,23,26,34)
                        and cum.status in (8,9,11,12,30) and c.course_id > 200
                left join one_to_one oto 
                    on oto.course_id = c.course_id 
                left join video_sessions_course_user_reports vscur 
                    on vscur.course_user_mapping_id = cum.course_user_mapping_id and oto.one_to_one_id = vscur.one_to_one_id
                where oto.one_to_one_type <> 7
                group by 1,2,3,4),
                
            mentor_session as 
                (select 
                        cum.user_id,
                        concat(ui.first_name,' ', ui.last_name) as student_name,
                        c.course_id,
                        cum.course_user_mapping_id,
                        max(vscur.join_time) filter (where vscur.overlapping_time_minutes >= 1) as last_one_to_one_attended_on
                    from
                        users_info ui
                    join course_user_mapping cum 
                        on cum.user_id = ui.user_id 
                    join courses c 
                        on c.course_id = cum.course_id  and c.course_structure_id in (1,6,8,11,12,13,14,18,19,20,22,23,26,34)
                            and cum.status in (8,9,11,12,30) and c.course_id > 200
                    left join one_to_one oto2
                        on oto2.course_id = c.course_id 
                    left join video_sessions_course_user_reports vscur 
                        on vscur.course_user_mapping_id = cum.course_user_mapping_id and oto2.one_to_one_id = vscur.one_to_one_id
                    where oto2.one_to_one_type = 7
                    group by 1,2,3,4),
                
            recorded_lectures as 
                (select 
                    cum.user_id,
                    concat(ui.first_name,' ', ui.last_name) as student_name,
                    c.course_id,
                    cum.course_user_mapping_id,
                    max(rlcur.lecture_watch_date) filter (where rlcur.total_time_watched_in_mins>= 1)  as last_lecture_watch_on
                from
                    users_info ui	
                join course_user_mapping cum 
                    on cum.user_id = ui.user_id 
                join courses c 
                    on c.course_id = cum.course_id and c.course_structure_id in (1,6,8,11,12,13,14,18,19,20,22,23,26,34)
                        and cum.status in (8,9,11,12,30) and c.course_id > 200
                left join lectures l
                    on l.course_id = c.course_id 
                left join recorded_lectures_course_user_reports rlcur 
                    on rlcur.lecture_id = l.lecture_id and rlcur.course_user_mapping_id = cum.course_user_mapping_id
                group by 1,2,3,4),
                
            group_sessions as 
                (select 
                    cum.user_id,
                    concat(ui.first_name,' ', ui.last_name) as student_name,
                    c.course_id,
                    cum.course_user_mapping_id,
                    max(group_session_course_user_reports.join_time) filter (where group_session_course_user_reports.overlapping_time_minutes >= 1)  as last_gs_date
                from
                    users_info ui	
                join course_user_mapping cum 
                    on cum.user_id = ui.user_id 
                join courses c 
                    on c.course_id = cum.course_id and c.course_structure_id in (1,6,8,11,12,13,14,18,19,20,22,23,26,34)
                        and cum.status in (8,9,11,12,30) and c.course_id > 200
                left join group_session_course_user_reports
                    on group_session_course_user_reports.course_user_mapping_id = cum.course_user_mapping_id
                group by 1,2,3,4)
                
            
            select
                concat(cum.user_id, c.course_id, cum.course_user_mapping_id) as table_unique_key, 
                cum.user_id,
                concat(ui.first_name,' ', ui.last_name) as student_name,
                c.course_id,
                c.course_name,
                c.course_structure_class,
                ui.lead_type,
                
                case 
                    when cum.label_id is null and cum.status in (8,9) then 'Enrolled Student'
                    when cum.label_id is not null and cum.status in (8,9) then 'Label Marked Student'
                    when c.course_structure_id in (1,18) and cum.status in (11,12) then 'ISA Cancelled Student'
                    when c.course_structure_id not in (1,18) and cum.status in (30) then 'Deferred Student'
                    when c.course_structure_id not in (1,18) and cum.status in (11) then 'Foreclosed Student'
                    when c.course_structure_id not in (1,18) and cum.status in (12) then 'Reject by NS-Ops'
                    else 'Mapping Error'
                end as label_mapping_status,
                course_user_category_mapping.student_category,
                
                greatest
                    (live_lectures.last_lecture_attended_on,assignments.last_question_attempted_on,assessments.last_question_attempted_on,
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
            
            join course_user_mapping cum 
                on cum.user_id = ui.user_id 
                
            join courses c 
                on c.course_id = cum.course_id and c.course_structure_id in (1,6,8,11,12,13,14,18,19,20,22,23,26,34)
                    and cum.status in (8,9,11,12,30) and c.course_id > 200 and lower(c.unit_type) like 'learning'
                    
            left join course_user_category_mapping
                on course_user_category_mapping.user_id = cum.user_id 
                    and c.course_id = course_user_category_mapping.course_id
                    
            left join live_lectures
                on live_lectures.user_id = cum.user_id 
                    and live_lectures.course_id = c.course_id 
            
            left join assignments
                on assignments.user_id = cum.user_id 
                    and assignments.course_id = c.course_id
            
            left join assessments
                on assessments.user_id = cum.user_id 
                    and assessments.course_id = c.course_id 
            
            left join one_to_one_cte
                on one_to_one_cte.user_id = cum.user_id 
                    and one_to_one_cte.course_id = c.course_id
            
            left join group_sessions
                on group_sessions.user_id = cum.user_id 
                    and group_sessions.course_id = c.course_id
            
            left join recorded_lectures
                on recorded_lectures.user_id = cum.user_id
                    and c.course_id = recorded_lectures.course_id
            
            left join mentor_session
                on mentor_session.course_id = c.course_id 
                    and cum.user_id = mentor_session.user_id;
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