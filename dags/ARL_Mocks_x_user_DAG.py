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
            'interviewer_no_show_unique)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
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
            'interviewer_no_show_unique = EXCLUDED.interviewer_no_show_unique;',
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
            interviewer_no_show_unique int
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_result_db',
    sql='''
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
            when one_to_one.one_to_one_type = 1 then 'Mock Technical Interview'
            when one_to_one.one_to_one_type = 2 then 'Mock HR Interview'
            when one_to_one.one_to_one_type = 3 then 'Mock Project'
            when one_to_one.one_to_one_type = 4 then 'Mock DSA'
            when one_to_one.one_to_one_type = 5 then 'Mock Full Stack'
            when one_to_one.one_to_one_type = 6 then 'Entrance Interview'
            when one_to_one.one_to_one_type = 7 then 'Mentor Catch Up'
            when one_to_one.one_to_one_type = 8 then 'Interview Coaching'
            when one_to_one.one_to_one_type = 9 then 'Mock Data Science'
            when one_to_one.one_to_one_type = 10 then 'General Interview'
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
        	then count(distinct one_to_one.one_to_one_id) else null
        end interviewer_declined_unique,
        case 
        	when dense_rank () over (partition by course_user_mapping.user_id ,one_to_one.one_to_one_id order by one_to_one_topic_mapping.topic_pool_id) = 1 
        	then count(distinct one_to_one.one_to_one_id) else null
        end confirmation_unique,
        case 
        	when dense_rank () over (partition by course_user_mapping.user_id ,one_to_one.one_to_one_id order by one_to_one_topic_mapping.topic_pool_id) = 1 
        	then count(distinct one_to_one.one_to_one_id) else null
        end student_cancellation_unique,
        case 
        	when dense_rank () over (partition by course_user_mapping.user_id ,one_to_one.one_to_one_id order by one_to_one_topic_mapping.topic_pool_id) = 1 
        	then count(distinct one_to_one.one_to_one_id) else null
        end interviewer_cancellation_unique,
        case 
        	when dense_rank () over (partition by course_user_mapping.user_id ,one_to_one.one_to_one_id order by one_to_one_topic_mapping.topic_pool_id) = 1 
        	then count(distinct one_to_one.one_to_one_id) else null
        end conducted_unique,
        case 
        	when dense_rank () over (partition by course_user_mapping.user_id ,one_to_one.one_to_one_id order by one_to_one_topic_mapping.topic_pool_id) = 1 
        	then count(distinct one_to_one.one_to_one_id) else null
        end cleared_unique,
        case 
        	when dense_rank () over (partition by course_user_mapping.user_id ,one_to_one.one_to_one_id order by one_to_one_topic_mapping.topic_pool_id) = 1 
        	then count(distinct one_to_one.one_to_one_id) else null
        end final_call_no_unique,
        case 
        	when dense_rank () over (partition by course_user_mapping.user_id ,one_to_one.one_to_one_id order by one_to_one_topic_mapping.topic_pool_id) = 1 
        	then count(distinct one_to_one.one_to_one_id) else null
        end final_call_maybe_unique,
        case 
        	when dense_rank () over (partition by course_user_mapping.user_id ,one_to_one.one_to_one_id order by one_to_one_topic_mapping.topic_pool_id) = 1 
        	then count(distinct one_to_one.one_to_one_id) else null
        end student_no_show_unique,
        case 
        	when dense_rank () over (partition by course_user_mapping.user_id ,one_to_one.one_to_one_id order by one_to_one_topic_mapping.topic_pool_id) = 1 
        	then count(distinct one_to_one.one_to_one_id) else null
        end interviewer_no_show_unique
    from
    	courses c
    join course_user_mapping
        on course_user_mapping.course_id = c.course_id and course_user_mapping.status in (8,9,11,12,30)
        	and c.course_structure_id in (1,6,8,11,12,13,14,18,19,20,22,23,26,34)
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
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17;
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