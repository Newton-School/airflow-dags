from airflow import DAG
# from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
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
            'INSERT INTO arl_mocks_x_user (table_unique_key,student_user_id,'
            'student_name, lead_type, student_category, user_enrollment_status,'
            'expert_user_id, course_id, course_structure_class, course_name,'
            'one_to_one_id, session_title,'
            'one_to_one_date, one_to_one_type, topic_pool_id, topic_pool_title,'
            'difficulty_level, scheduled, pending_confirmation, '
            'interviewer_declined, confirmation,'
            'student_cancellation, interviewer_cancellation, conducted,cleared,'
            'final_call_no, final_call_maybe, student_no_show, interviewer_no_show,'
            'scheduled_unique,'
            'pending_confirmation_unique,'
            'interviewer_declined_unique,'
            'confirmation_unique,'
            'student_cancellation_unique,'
            'interviewer_cancellation_unique,'
            'conducted_unique,'
            'cleared_unique,'
            'final_call_no_unique,'
            'final_call_maybe_unique,'
            'student_no_show_unique,'
            'interviewer_no_show_unique,'
            'activity_status_7_days,'
            'activity_status_14_days,'
            'activity_status_30_days,'
            'expert_join_time,'
            'expert_leave_time,'
            'user_join_time,'
            'user_leave_time,'
            'total_expert_time_mins,'
            'total_user_time_mins,'
            'total_overlapping_time_mins,'
            'cancel_reason,'
            'user_placement_status,'
            'answer_rating,'
            'rating_feedback_answer,'
            'admin_course_id,'
            'admin_unit_name)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'
            '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (table_unique_key) do update set student_name = EXCLUDED.student_name,'
            'lead_type = EXCLUDED.lead_type,'
            'student_category = EXCLUDED.student_category,'
            'user_enrollment_status = EXCLUDED.user_enrollment_status,'
            'course_structure_class = EXCLUDED.course_structure_class,'
            'course_name = EXCLUDED.course_name,'
            'session_title = EXCLUDED.session_title,'
            'topic_pool_title = EXCLUDED.topic_pool_title,'
            'difficulty_level = EXCLUDED.difficulty_level,'
            'scheduled = EXCLUDED.scheduled,'
            'pending_confirmation = EXCLUDED.pending_confirmation,'
            'interviewer_declined = EXCLUDED.interviewer_declined,'
            'confirmation = EXCLUDED.confirmation,'
            'student_cancellation = EXCLUDED.student_cancellation,'
            'interviewer_cancellation = EXCLUDED.interviewer_cancellation,'
            'conducted = EXCLUDED.conducted,'
            'cleared = EXCLUDED.cleared,'
            'final_call_no = EXCLUDED.final_call_no,'
            'final_call_maybe = EXCLUDED.final_call_maybe,'
            'student_no_show = EXCLUDED.student_no_show,'
            'interviewer_no_show = EXCLUDED.interviewer_no_show,'
            'scheduled_unique = EXCLUDED.scheduled_unique,'
            'pending_confirmation_unique = EXCLUDED.pending_confirmation_unique,'
            'interviewer_declined_unique = EXCLUDED.interviewer_declined_unique,'
            'confirmation_unique = EXCLUDED.confirmation_unique,'
            'student_cancellation_unique = EXCLUDED.student_cancellation_unique,'
            'interviewer_cancellation_unique = EXCLUDED.interviewer_cancellation_unique,'
            'conducted_unique = EXCLUDED.conducted_unique,'
            'cleared_unique = EXCLUDED.cleared_unique,'
            'final_call_no_unique = EXCLUDED.final_call_no_unique,'
            'final_call_maybe_unique = EXCLUDED.final_call_maybe_unique,'
            'student_no_show_unique = EXCLUDED.student_no_show_unique,'
            'interviewer_no_show_unique = EXCLUDED.interviewer_no_show_unique,'
            'activity_status_7_days = EXCLUDED.activity_status_7_days,'
            'activity_status_14_days = EXCLUDED.activity_status_14_days,'
            'activity_status_30_days = EXCLUDED.activity_status_30_days,'
            'expert_join_time = EXCLUDED.expert_join_time,'
            'expert_leave_time= EXCLUDED.expert_leave_time,'
            'user_join_time = EXCLUDED.user_join_time,'
            'user_leave_time = EXCLUDED.user_leave_time,'
            'total_expert_time_mins = EXCLUDED.total_expert_time_mins,'
            'total_user_time_mins = EXCLUDED.total_user_time_mins,'
            'total_overlapping_time_mins = EXCLUDED.total_overlapping_time_mins,'
            'cancel_reason = EXCLUDED.cancel_reason,'
            'user_placement_status = EXCLUDED.user_placement_status,'
            'answer_rating = EXCLUDED.answer_rating,'
            'rating_feedback_answer = EXCLUDED.rating_feedback_answer,'
            'admin_course_id = EXCLUDED.admin_course_id,'
            'admin_unit_name = EXCLUDED.admin_unit_name;',
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
                transform_row[21],
                transform_row[22],
                transform_row[23],
                transform_row[24],
                transform_row[25],
                transform_row[26],
                transform_row[27],
                transform_row[28],
                transform_row[29],
                transform_row[30],
                transform_row[31],
                transform_row[32],
                transform_row[33],
                transform_row[34],
                transform_row[35],
                transform_row[36],
                transform_row[37],
                transform_row[38],
                transform_row[39],
                transform_row[40],
                transform_row[41],
                transform_row[42],
                transform_row[43],
                transform_row[44],
                transform_row[45],
                transform_row[46],
                transform_row[47],
                transform_row[48],
                transform_row[49],
                transform_row[50],
                transform_row[51],
                transform_row[52],
                transform_row[53],
                transform_row[54],
                transform_row[55],
                transform_row[56],

            )
        )
    pg_conn.commit()


dag = DAG(
    'ARL_Mocks_x_User',
    default_args=default_args,
    concurrency=4,
    max_active_tasks=6,
    max_active_runs=6,
    description='An Analytics Reporting Layer DAG for Mocks x user',
    schedule_interval='30 0 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS arl_mocks_x_user (
            id serial,
            table_unique_key text not null PRIMARY KEY,
            student_user_id bigint,
            student_name text,
            lead_type text,
            student_category text,
            user_enrollment_status text,
            expert_user_id bigint,
            course_id int,
            course_structure_class text,
            course_name text,
            one_to_one_id bigint,
            session_title text,
            one_to_one_date date,
            one_to_one_type text,
            topic_pool_id int,
            topic_pool_title text,
            difficulty_level text,
            scheduled int,
            pending_confirmation int,
            interviewer_declined int,
            confirmation int,
            student_cancellation int,
            interviewer_cancellation int,
            conducted int,
            cleared int,
            final_call_no int,
            final_call_maybe int,
            student_no_show int,
            interviewer_no_show int,
            scheduled_unique int, 
            pending_confirmation_unique int,
            interviewer_declined_unique int,
            confirmation_unique int,
            student_cancellation_unique int, 
            interviewer_cancellation_unique int,
            conducted_unique int,
            cleared_unique int,
            final_call_no_unique int,
            final_call_maybe_unique int, 
            student_no_show_unique int, 
            interviewer_no_show_unique int,
            activity_status_7_days text,
            activity_status_14_days text,
            activity_status_30_days text,
            expert_join_time timestamp, 
            expert_leave_time timestamp, 
            user_join_time timestamp,
            user_leave_time timestamp,
            total_expert_time_mins real,
            total_user_time_mins real,
            total_overlapping_time_mins real,
            cancel_reason text,
            user_placement_status text,
            answer_rating int,
            rating_feedback_answer text,
            admin_course_id int,
            admin_unit_name text
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_result_db',
    sql='''
        with time_data as 
            (with vscur as 
                (select 
                    one_to_one_id,
                    user_id,
                    course_user_mapping_id,
                    join_time,
                    leave_time,
                    extract('epoch' from (leave_time - join_time)) as time_diff_in_secs,
                    one_to_one_type,
                    stakeholder_type,
                    overlapping_time_seconds,
                    overlapping_time_minutes
                from
                    video_sessions_one_to_one_course_user_reports vscur 
                group by 1,2,3,4,5,6,7,8,9,10)
    
        select
            oto.one_to_one_id,
            oto.course_id,
            oto.student_user_id,
            oto.expert_user_id,
            date(oto.one_to_one_start_timestamp) as session_date,
            case 
                when oto.one_to_one_type = 1 then 'Technical Mock'	
                when oto.one_to_one_type = 2 then 'HR Mock'
                when oto.one_to_one_type = 3 then 'Project Mock'
                when oto.one_to_one_type = 4 then 'DSA Mock' 
                when oto.one_to_one_type = 5 then 'Mock Full stack'
                when oto.one_to_one_type = 7 then 'One-On-One Session'
                when oto.one_to_one_type = 9 then 'Data Science - Mock'
                when oto.one_to_one_type = 11 then 'Mentor Help - Doubt Session'
                when oto.one_to_one_type = 12 then 'FrontEnd - Mock Interview'
                when oto.one_to_one_type = 13 then 'BackEnd - Mock Interview'
                when oto.one_to_one_type = 14 then 'FrontEnd - JS Mock Interview'
                when oto.one_to_one_type = 15 then 'FrontEnd - ReactJS Mock Interview'
            end as session_type,
            oto.cancel_reason,
            min(vscur.join_time) filter (where stakeholder_type like 'Expert') as expert_join_time,
            min(vscur.join_time) filter (where stakeholder_type like 'User') as user_join_time,
            max(vscur.leave_time) filter (where stakeholder_type like 'Expert') as expert_leave_time,
            max(vscur.leave_time) filter (where stakeholder_type like 'User') as user_leave_time,
            sum(time_diff_in_secs) filter (where stakeholder_type like 'Expert') / 60 as total_expert_time_mins,
            sum(time_diff_in_secs) filter (where stakeholder_type like 'User') / 60 as total_user_time_mins,
            sum(overlapping_time_minutes) filter (where stakeholder_type like 'User') as total_overlapping_time_mins
        from
            one_to_one oto
        left join vscur
            on vscur.one_to_one_id = oto.one_to_one_id
        group by 1,2,3,4,5,6,7
        order by 5 desc),
        
        one_on_one_csat as 
			(select
				user_id,
				entity_object_id as one_to_one_id,
			    case
			        when ffar.feedback_answer = 'Awesome' then 5
			        when ffar.feedback_answer = 'Good' then 4
			        when ffar.feedback_answer = 'Average' then 3
			        when ffar.feedback_answer = 'Poor' then 2
			        when ffar.feedback_answer = 'Very Poor' then 1
			    end as answer_rating,
			    feedback_answer as rating_feedback_answer
			from
				feedback_form_all_responses_new ffar
			where ffar.feedback_form_id = 4414 
				and ffar.feedback_question_id = 262
			group by 1,2,3,4)
    

    select distinct 
            concat(course_user_mapping.user_id,c.course_id,one_to_one.expert_user_id,one_to_one.one_to_one_id, EXTRACT(month FROM date(one_to_one.one_to_one_start_timestamp)),EXTRACT(year FROM date(one_to_one.one_to_one_start_timestamp)),one_to_one.one_to_one_type,one_to_one_topic_mapping.topic_pool_id,EXTRACT(day FROM date(one_to_one.one_to_one_start_timestamp)),one_to_one.difficulty_level) as table_unique_key,
            course_user_mapping.user_id,
            concat(ui.first_name,' ', ui.last_name) as student_name,
            ui.lead_type,
            cucm.student_category,
            case
                when course_user_mapping.label_id is null and course_user_mapping.status in (8,9) then 'Enrolled Student'
                when course_user_mapping.label_id is not null and course_user_mapping.status in (8,9) then 'Label Marked Student'
                when c.course_structure_id in (1,18) and course_user_mapping.status in (11,12) then 'ISA Cancelled Student'
                when c.course_structure_id not in (1,18) and course_user_mapping.status in (30) then 'Deferred Student'
                when c.course_structure_id not in (1,18) and course_user_mapping.status in (11) then 'Foreclosed Student'
                when c.course_structure_id not in (1,18) and course_user_mapping.status in (12) then 'Reject by NS-Ops'
                else 'Mapping Error'
            end as user_enrollment_status,
            one_to_one.expert_user_id,
            c.course_id,
            c.course_structure_class,
            c.course_name,
            one_to_one.one_to_one_id,
            one_to_one.title as session_title,
            date(one_to_one.one_to_one_start_timestamp) as one_to_one_date,
            case 
                when one_to_one.one_to_one_type = 1 then 'Technical Mock'	
                when one_to_one.one_to_one_type = 2 then 'HR Mock'
                when one_to_one.one_to_one_type = 3 then 'Project Mock'
                when one_to_one.one_to_one_type = 4 then 'DSA Mock' 
                when one_to_one.one_to_one_type = 5 then 'Mock Full stack'
                when one_to_one.one_to_one_type = 6 then 'Entrance Interview'
                when one_to_one.one_to_one_type = 7 then 'One-On-One Session'
                when one_to_one.one_to_one_type = 8 then 'Interview Coaching'
                when one_to_one.one_to_one_type = 9 then 'Data Science - Mock'
                when one_to_one.one_to_one_type = 10 then 'General Interview'
                when one_to_one.one_to_one_type = 11 then 'Mentor Help - Doubt Session'
                when one_to_one.one_to_one_type = 12 then 'FrontEnd - Mock Interview'
                when one_to_one.one_to_one_type = 13 then 'BackEnd - Mock Interview'
                when one_to_one.one_to_one_type = 14 then 'FrontEnd - JS Mock Interview'
                when one_to_one.one_to_one_type = 15 then 'FrontEnd - ReactJS Mock Interview'
            end as one_to_one_type,
            one_to_one_topic_mapping.topic_pool_id,
            topic_pool_mapping.topic_pool_title,
            case 
                when one_to_one.difficulty_level = 1 then 'Beginner'
                when one_to_one.difficulty_level = 2 then 'Easy'
                when one_to_one.difficulty_level = 3 then 'Medium'
                when one_to_one.difficulty_level = 4 then 'Hard'
                when one_to_one.difficulty_level = 5 then 'Challenge' else null 
            end as difficulty_level,
            count(distinct one_to_one.one_to_one_id) as scheduled,
            count(distinct one_to_one.one_to_one_id) filter (where one_to_one.one_to_one_status = 1) as pending_confirmation,
            count(distinct one_to_one.one_to_one_id) filter (where one_to_one.one_to_one_status = 3) as interviewer_declined,
            count(distinct one_to_one.one_to_one_id) filter (where one_to_one.one_to_one_status = 2) as confirmation,
            count(distinct one_to_one.one_to_one_id) filter (where one_to_one.one_to_one_status = 5) as student_cancellation,
            count(distinct one_to_one.one_to_one_id) filter (where one_to_one.one_to_one_status = 4) as interviewer_cancellation,
            count(distinct one_to_one.one_to_one_id) filter (where one_to_one.one_to_one_status = 2 and one_to_one.final_call in (1,2,3)) as conducted,
            count(distinct one_to_one.one_to_one_id) filter (where one_to_one.one_to_one_status = 2 and one_to_one.final_call = 1) as cleared,
            count(distinct one_to_one.one_to_one_id) filter (where one_to_one.one_to_one_status = 2 and one_to_one.final_call = 2) as final_call_no,
            count(distinct one_to_one.one_to_one_id) filter (where one_to_one.one_to_one_status = 2 and one_to_one.final_call = 3) as final_call_maybe,
            count(distinct one_to_one.one_to_one_id) filter (where one_to_one.one_to_one_status = 10 and (one_to_one.cancel_reason like 'Insufficient time spent by booked by user%' or one_to_one.cancel_reason like 'Insufficient overlap time')) as student_no_show,
            count(distinct one_to_one.one_to_one_id) filter (where one_to_one.one_to_one_status = 10 and one_to_one.cancel_reason like 'Insufficient time spent by booked with user%') as interviewer_no_show,
            case 
                when dense_rank () over (partition by course_user_mapping.user_id ,one_to_one.one_to_one_id order by one_to_one_topic_mapping.topic_pool_id) = 1 
                then count(distinct one_to_one.one_to_one_id) else null
            end scheduled_unique,
            case 
                when dense_rank () over (partition by course_user_mapping.user_id ,one_to_one.one_to_one_id order by one_to_one_topic_mapping.topic_pool_id) = 1 
                then count(distinct one_to_one.one_to_one_id) filter (where one_to_one.one_to_one_status = 1) else null
            end pending_confirmation_unique,
            case 
                when dense_rank () over (partition by course_user_mapping.user_id ,one_to_one.one_to_one_id order by one_to_one_topic_mapping.topic_pool_id) = 1 
                then count(distinct one_to_one.one_to_one_id) filter (where one_to_one.one_to_one_status = 3) else null
            end interviewer_declined_unique,
            case 
                when dense_rank () over (partition by course_user_mapping.user_id ,one_to_one.one_to_one_id order by one_to_one_topic_mapping.topic_pool_id) = 1 
                then count(distinct one_to_one.one_to_one_id) filter (where one_to_one.one_to_one_status = 2) else null
            end confirmation_unique,
            case 
                when dense_rank () over (partition by course_user_mapping.user_id ,one_to_one.one_to_one_id order by one_to_one_topic_mapping.topic_pool_id) = 1 
                then count(distinct one_to_one.one_to_one_id) filter (where one_to_one.one_to_one_status = 5) else null
            end student_cancellation_unique,
            case 
                when dense_rank () over (partition by course_user_mapping.user_id ,one_to_one.one_to_one_id order by one_to_one_topic_mapping.topic_pool_id) = 1 
                then count(distinct one_to_one.one_to_one_id) filter (where one_to_one.one_to_one_status = 4) else null
            end interviewer_cancellation_unique,
            case 
                when dense_rank () over (partition by course_user_mapping.user_id ,one_to_one.one_to_one_id order by one_to_one_topic_mapping.topic_pool_id) = 1 
                then count(distinct one_to_one.one_to_one_id) filter (where one_to_one.one_to_one_status = 2 and one_to_one.final_call in (1,2,3)) else null
            end conducted_unique,
            case 
                when dense_rank () over (partition by course_user_mapping.user_id ,one_to_one.one_to_one_id order by one_to_one_topic_mapping.topic_pool_id) = 1 
                then count(distinct one_to_one.one_to_one_id) filter (where one_to_one.one_to_one_status = 2 and one_to_one.final_call = 1) else null
            end cleared_unique,
            case 
                when dense_rank () over (partition by course_user_mapping.user_id ,one_to_one.one_to_one_id order by one_to_one_topic_mapping.topic_pool_id) = 1 
                then count(distinct one_to_one.one_to_one_id) filter (where one_to_one.one_to_one_status = 2 and one_to_one.final_call = 2) else null
            end final_call_no_unique,
            case 
                when dense_rank () over (partition by course_user_mapping.user_id ,one_to_one.one_to_one_id order by one_to_one_topic_mapping.topic_pool_id) = 1 
                then count(distinct one_to_one.one_to_one_id) filter (where one_to_one.one_to_one_status = 2 and one_to_one.final_call = 3) else null
            end final_call_maybe_unique,
            case 
                when dense_rank () over (partition by course_user_mapping.user_id ,one_to_one.one_to_one_id order by one_to_one_topic_mapping.topic_pool_id) = 1 
                then count(distinct one_to_one.one_to_one_id) filter (where one_to_one.one_to_one_status = 10 and (one_to_one.cancel_reason like 'Insufficient time spent by booked by user%' or one_to_one.cancel_reason like 'Insufficient overlap time')) else null
            end student_no_show_unique,
            case 
                when dense_rank () over (partition by course_user_mapping.user_id ,one_to_one.one_to_one_id order by one_to_one_topic_mapping.topic_pool_id) = 1 
                then count(distinct one_to_one.one_to_one_id) filter (where one_to_one.one_to_one_status = 10 and one_to_one.cancel_reason like 'Insufficient time spent by booked with user%') else null
            end interviewer_no_show_unique,
            uasm.activity_status_7_days,
            uasm.activity_status_14_days,
            uasm.activity_status_30_days,
            expert_join_time,
            expert_leave_time,
            user_join_time,
            user_leave_time,
            time_data.total_expert_time_mins,
            time_data.total_user_time_mins,
            total_overlapping_time_mins,
            one_to_one.cancel_reason,
            course_user_mapping.user_placement_status,
            one_on_one_csat.answer_rating,
            one_on_one_csat.rating_feedback_answer,
            course_user_mapping.admin_course_id,
            course_user_mapping.admin_unit_name
        from
            courses c
        join course_user_mapping
            on course_user_mapping.course_id = c.course_id and course_user_mapping.status in (8,9,11,12,30)
                and c.course_structure_id in (1,6,8,11,12,13,14,18,19,20,22,23,26,34,50,51,52,53,54,55,56,57,58,59,60,63,64,65,66,67)
        left join one_to_one 
            on c.course_id = one_to_one.course_id and course_user_mapping.user_id = one_to_one.student_user_id
        left join course_user_category_mapping cucm
            on cucm.course_id = c.course_id and course_user_mapping.user_id = cucm.user_id
        left join users_info ui
            on ui.user_id = course_user_mapping.user_id
        left join one_to_one_topic_mapping
            on one_to_one_topic_mapping.one_to_one_id = one_to_one.one_to_one_id
        left join topic_pool_mapping
            on topic_pool_mapping.topic_pool_id = one_to_one_topic_mapping.topic_pool_id
        left join user_activity_status_mapping uasm 
            on uasm.user_id = course_user_mapping.user_id
        left join time_data
        	on time_data.one_to_one_id = one_to_one.one_to_one_id
        		and time_data.student_user_id = course_user_mapping.user_id
        			and time_data.expert_user_id = one_to_one.expert_user_id
        left join one_on_one_csat 
        	on one_on_one_csat.one_to_one_id = one_to_one.one_to_one_id 
        		and one_to_one.student_user_id = one_on_one_csat.user_id
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57;
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