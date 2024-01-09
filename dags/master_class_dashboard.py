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
            'INSERT INTO master_class_dashboard (email,lead_created_on,lecture_date,overlapping_time_minutes,prospect_stage,was_prospect,prospect_date,first_connect,salary,degree,twelfth_marks,lead_owner,instructor_name,number_of_cnc_dials,docs,inst_time_in_mins,rfd_date,offer_letter_date,lecture_prospect_status,lecture_before_rfd,lecture_first_connect_status,rfd_and_lecture_same_month,test_marks,test_date,not_interested_reason)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);',
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
            )
        )
    pg_conn.commit()


dag = DAG(
    'Master_Class_Dashboard_DAG',
    default_args=default_args,
    description='An Analytics Reporting Layer DAG for Master Class Dashboard',
    schedule_interval='40 2 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS master_class_dashboard (
            id serial,
            email varchar(256),
            lead_created_on DATE,
            lecture_date DATE,
            overlapping_time_minutes real,
            prospect_stage varchar(256),
            was_prospect varchar(256),
            prospect_date DATE,
            first_connect DATE,
            salary  varchar(256),
            degree varchar(256),
            twelfth_marks varchar(256),
            lead_owner varchar(256),
            instructor_name varchar(256),
            number_of_cnc_dials int,
            docs boolean,
            inst_time_in_mins real,
            rfd_date DATE,
            offer_letter_date DATE,
            lecture_prospect_status varchar(256),
            lecture_before_rfd boolean,
            lecture_first_connect_status varchar(256),
            rfd_and_lecture_same_month boolean,
            test_marks int,
            test_date DATE,
            not_interested_reason  varchar(256)
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_result_db',
    sql='''
            with test_taken_1 as(
        select 
                    distinct users_info.email,
                    date(assessment_started_at) as test_date,
                    max(marks_obtained) as marks_obtained,
                    count(distinct aqum.mcq_id) filter (where aqum.id is not null) as total_mcqs_opened,
                    count(distinct aqum.mcq_id) filter (where aqum.option_marked_at is not null) as total_mcqs_attempted,
                    count(distinct aqum.mcq_id) filter (where aqum.marked_choice = aqum.correct_choice) as total_mcqs_correct
                from
                    assessments a 
                join courses c 
                    on c.course_id = a.course_id and c.course_id in (800,818,819,820)
                left join course_user_mapping as cum on cum.course_id = c.course_id
                left join assessment_question_user_mapping aqum 
                    on aqum.assessment_id = a.assessment_id 
                        and aqum.course_user_mapping_id = cum.course_user_mapping_id 
                left join users_info on users_info.user_id = aqum.user_id
                where users_info.email not like ('%@newtonschool.co%')
                group by 1,2
        ),
        test_taken_2 as(
        select 
                    distinct users_info.email,
                    date(assessment_started_at) as test_date,
                    max(marks_obtained) as marks_obtained,
                    count(distinct aqum.mcq_id) filter (where aqum.id is not null) as total_mcqs_opened,
                    count(distinct aqum.mcq_id) filter (where aqum.option_marked_at is not null) as total_mcqs_attempted,
                    count(distinct aqum.mcq_id) filter (where aqum.marked_choice = aqum.correct_choice) as total_mcqs_correct
                from
                    assessments a 
                join courses c 
                    on c.course_id = a.course_id and c.course_id in (803)
                left join course_user_mapping as cum on cum.course_id = c.course_id
                left join assessment_question_user_mapping aqum 
                    on aqum.assessment_id = a.assessment_id 
                        and aqum.course_user_mapping_id = cum.course_user_mapping_id 
                left join users_info on users_info.user_id = aqum.user_id
                where users_info.email not like ('%@newtonschool.co%')
                group by 1,2
        
        ),
        test_taken_3 as (
        select * from test_taken_1
        union all
        select * from test_taken_2
        ),
        test_taken as(
        select 
                    distinct email,
                    max(test_date) as test_date,
                    max(marks_obtained) as marks_obtained,
                    max(total_mcqs_opened) as total_mcqs_opened,
                    max(total_mcqs_attempted) as total_mcqs_attempted,
                    max(total_mcqs_correct) as total_mcqs_correct
                from test_taken_3
                group by 1
                order by 3 desc
        ),
        raw as(
        select
        distinct 
        users_info.email,
        apply_forms_and_questions.question_text,
        response as apply_form_response,
        case 
        when course_user_timeline_flow_mapping.apply_form_question_set = 1 then 'Default'
        when course_user_timeline_flow_mapping.apply_form_question_set = 2 then 'Long' end as apply_form_question_set,
        -- date(course_user_mapping.created_at) as created_at,
        date(users_info.date_joined) as date_joined
        from users_info
        left join course_user_timeline_flow_mapping on course_user_timeline_flow_mapping.user_id = users_info.user_id and course_user_timeline_flow_mapping.course_id in (786,759,800,818,819,820)
        left join apply_form_course_user_question_mapping on apply_form_course_user_question_mapping.user_id = course_user_timeline_flow_mapping.user_id and apply_form_course_user_question_mapping.course_id = course_user_timeline_flow_mapping.course_id
        left join apply_forms_and_questions on apply_forms_and_questions.apply_form_question_id = apply_form_course_user_question_mapping.apply_form_question_id
        -- left join course_user_mapping on course_user_mapping.user_id = course_user_timeline_flow_mapping.user_id and course_user_timeline_flow_mapping.course_id = course_user_mapping.course_id
        left join courses on courses.course_id = course_user_timeline_flow_mapping.course_id and courses.course_id in (786,759,800,818,819,820)
        where courses.course_id in (786,759,800,818,819,820)
        order by 1
        ),
        b as(
        select
        distinct email,
        case when question_text = 'What are you doing currently?' then apply_form_response end as "What are you doing currently?",
        case when question_text = 'Graduation Year (College passing out year)' then apply_form_response end as "Graduation Year (College passing out year)",
        case when question_text in ('Designation','How much salary do you get in a month currently?','What is your total yearly salary package? (LPA - Lakh Per Annum)') then apply_form_response end as "salary",
        case when question_text in ('Please mention your Work Experience in years') then apply_form_response end as "Work_ex",
        case when question_text in ('Current Location (City)') then apply_form_response end as "current_location",
        case when question_text in ('Why do you want to join the Data Science course?') then apply_form_response end as "why_do_you_want_to_join",
        case when question_text in ('What degree did you graduate in?') then apply_form_response end as "degree",
        case when question_text in ('12th Passing Marks (in Percentage)') then apply_form_response end as "12th"
        from raw
        ),
        final as(
        select
        distinct email,
        max("salary") as salary,
        max("why_do_you_want_to_join") as why_do_you_want_to_join,
        max("degree") as degree,
        max("12th") as twelfth_marks,
        max("Graduation Year (College passing out year)") as graduation_year,
        max("What are you doing currently?") as life_status
        from b
        group by 1
        ),
        lecture_details_final as(
        with inst_time_raw as 
            (select distinct
                lectures.lecture_id,
                cum.user_id, 
                let.course_user_mapping_id,
                join_time,
                leave_time,
                extract('epoch' from leave_time - join_time) / 60 as time_mins
            from
                courses c 
            join lectures
                on lectures.course_id = c.course_id and c.course_id = 644
            join course_user_mapping cum 
                on cum.course_id = lectures.course_id
            join lecture_engagement_time let 
                on let.lecture_id = lectures.lecture_id 
                    and cum.course_user_mapping_id = let.course_user_mapping_id 
                        and cum.user_id = lectures.instructor_user_id
            where date_trunc('month',date(lectures.start_timestamp)) >= date_trunc('month',now()) - interval '3 month'			
            order by 1,2),
        inst_time as 
            (select
                lecture_id,
                user_id,
                course_user_mapping_id,
                sum(time_mins) as inst_staying_time_mintues
            from
                inst_time_raw
            group by 1,2,3)
        select
            date(lectures.start_timestamp) as lecture_date,
            concat(ui2.first_name,' ', ui2.last_name) as inst_name,
            ui.email as student_email,
            cast(inst_time.inst_staying_time_mintues as float) as inst_time_in_mins,
            cast(sum(let.overlapping_time_minutes) as float) as user_ovrlap_time_in_mins
        from
            lectures
        join courses c
            on c.course_id = lectures.course_id and lectures.course_id = 644
        join course_user_mapping cum 
            on cum.course_id = c.course_id 
        join lecture_engagement_time let 
            on let.lecture_id = lectures.lecture_id 
                and cum.course_user_mapping_id = let.course_user_mapping_id 
                    and lower(let.user_type) like 'user'
        left join inst_time
            on inst_time.lecture_id = lectures.lecture_id
        left join users_info ui
            on ui.user_id = cum.user_id
        left join users_info ui2
            on ui2.user_id = lectures.instructor_user_id
        where date_trunc('month',date(lectures.start_timestamp)) >= date_trunc('month',now()) - interval '3 month'	
        group by 1,2,3,4
        ),
        lecture_details as(
        select
        distinct lecture_date,
        student_email as email,
        inst_name as instructor_name,
        inst_time_in_mins as inst_time_in_mins,
        user_ovrlap_time_in_mins as overlapping_time_minutes
        from lecture_details_final
        ),
        all_time_prospect as(
        select
        distinct email_address,
        true as a_t_prospect,
        min(date(modified_on)) as prospect_date
        from lsq_leads_x_activities
        where current_stage = 'Prospect'
        group by 1,2
        ),
        first_connect as(
        select
        distinct email_address,
        min(date(modified_on)) as first_connect
        from lecture_details
        left join lsq_leads_x_activities on lsq_leads_x_activities.email_address = lecture_details.email
        where lead_created_on is not null and lead_owner not in ('System','Jai Sharma','Praduman Goyal')
        and ((event in ('Outbound Phone Call Activity') and call_type = 'Answered') or (event in ('Log Phone Call') and mx_custom_1 not in ('CNC','CBL'))) and event in ('Outbound Phone Call Activity','Log Phone Call')
        group by 1
        order by email_address
        ),
        docs as(
        select
        distinct email_address,
        true as docs_collected
        from lsq_leads_x_activities
        where event in ('Document / Payment Tracking')
        ),
        open_leads as(
        select
        distinct email_address,
        lsq_leads_x_activities.lead_owner,
        count(distinct activity_id) filter (where ((event in ('Outbound Phone Call Activity') and call_type = 'NotAnswered') or (event in ('Log Phone Call') and mx_custom_1 in ('CNC'))) and event in ('Outbound Phone Call Activity','Log Phone Call')) as number_of_cnc_dials
        from final
        left join lsq_leads_x_activities on lsq_leads_x_activities.email_address = final.email
        where lead_created_on is not null and lead_owner not in ('System','Jai Sharma','Praduman Goyal')
        group by 1,2
        order by email_address
        ),
        offer_letter as(
        select
        distinct email_address,
        true as ol,
        date(modified_on) as offer_letter_date
        from lsq_leads_x_activities
        where event = 'Sent Offer Letter'
        )
        select
        distinct lecture_details.email,
        date(lsq_leads_x_activities.lead_created_on) as lead_created_on,
        lecture_date,
        overlapping_time_minutes,
        prospect_stage,
        case when all_time_prospect.a_t_prospect is true then 'Yes' else null end as was_prospect,
        prospect_date,
        first_connect,
        salary,
        degree,
        twelfth_marks,
        lsq_leads_x_activities.lead_owner,
        instructor_name,
        number_of_cnc_dials,
        case when docs_collected is true then true else false end as docs,
        inst_time_in_mins,
        date(mx_rfd_date) as rfd_date,
        offer_letter_date,
        case 
        when date(lecture_date) >= date(prospect_date) is null then 'Lecture After Prospect'
        when date(prospect_date) is null then 'Lecture Before Prospect'
        when date(lecture_date) < date(prospect_date) is null then 'Lecture Before Prospect'
        else 'Lecture Before Prospect'
        end as lecture_prospect_status,
        
        case 
        when date(lecture_date) >= date(mx_rfd_date) is null then false
        when date(mx_rfd_date) is null then true
        when date(lecture_date) < date(mx_rfd_date) is null then true
        else true
        end as lecture_before_rfd,
        
        case
        when date(lecture_date) >= date(first_connect) is null then 'Lecture After 1st Connect'
        when date(first_connect) is null then 'Lecture Before 1st Connect'
        when date(lecture_date) < date(first_connect) is null then 'Lecture Before 1st Connect'
        else 'Lecture Before 1st Connect'
        end as lecture_first_connect_status,
        
        case
        when date_trunc('month',date(mx_rfd_date)) = date_trunc('month',now()) then true else false end rfd_and_lecture_same_month,
        
        test_taken.marks_obtained as test_marks,
        test_taken.test_date,
        case 
        when lsq_leads_x_activities.prospect_stage = 'Rejected'
        and (lsq_leads_x_activities.mx_custom_3 IS NOT NULL)
        and ((lsq_leads_x_activities.mx_custom_3 <> '') or (lsq_leads_x_activities.mx_custom_3 is null))
        AND (lsq_leads_x_activities.event_name = 'Log Phone Call') then lsq_leads_x_activities.mx_custom_3 else null end as not_interested_reason
        
        from lecture_details
        left join lsq_leads_x_activities on lsq_leads_x_activities.email_address = lecture_details.email
        left join final on final.email = lecture_details.email
        left join all_time_prospect on all_time_prospect.email_address = lecture_details.email
        left join first_connect on first_connect.email_address = lecture_details.email
        left join open_leads on open_leads.email_address = lecture_details.email
        left join docs on docs.email_address = final.email
        left join offer_letter on offer_letter.email_address = final.email
        left join test_taken on test_taken.email = final.email
        where lsq_leads_x_activities.lead_owner not in ('System','Jai Sharma','Praduman Goyal','Divvya Sinha')
        order by 1,3,4
    ;
        ''',
    dag=dag
)
drop_table = PostgresOperator(
    task_id='drop_table',
    postgres_conn_id='postgres_result_db',
    sql='''DROP TABLE IF EXISTS master_class_dashboard;
    ''',
    dag=dag
)

extract_python_data = PythonOperator(
    task_id='extract_python_data',
    python_callable=extract_data_to_nested,
    provide_context=True,
    dag=dag
)
drop_table >> create_table >> transform_data >> extract_python_data