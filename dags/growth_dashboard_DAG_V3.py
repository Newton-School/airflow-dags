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
            'INSERT INTO growth_dashboard_v3 ('
                'email,course_timeline_flow,cum_created_at,date_joined,'
                'cutfm_created_at,prospect_date,course_id,created_at,churned_date,salary,why_do_you_want_to_join,'
                'degree,twelfth_marks,graduation_year,life_status,prospect_stage,icp_status,was_prospect,ol,'
                'paid_on_product,live_class,lead_owner,number_of_dials_prospect,number_of_dials,'
                'number_of_dials_attempted,number_of_connects,paid_on_product_and_organic,docs,responded_for_want_a_call,'
                'lead_quality,rfd_date,marks_obtained,test_date,total_mcqs_attempted,'
                'utm_source,utm_medium,utm_campaign,source,lead_last_call_status)'
                'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                'on conflict (email) do update set'
                'email = growth_dashboard_v3.email',
                'course_timeline_flow = EXCLUDED.course_timeline_flow',
                'cum_created_at = EXCLUDED.cum_created_at',
                'date_joined = growth_dashboard_v3.date_joined',
                'cutfm_created_at = growth_dashboard_v3.cutfm_created_at',
                'prospect_date = EXCLUDED.prospect_date',
                'course_id = EXCLUDED.course_id',
                'created_at = EXCLUDED.created_at',
                'churned_date = EXCLUDED.churned_date',
                'salary = EXCLUDED.salary',
                'why_do_you_want_to_join = EXCLUDED.why_do_you_want_to_join',
                'degree = EXCLUDED.degree',
                'twelfth_marks = EXCLUDED.twelfth_marks',
                'graduation_year = EXCLUDED.graduation_year',
                'life_status = EXCLUDED.life_status',
                'prospect_stage = EXCLUDED.prospect_stage',
                'icp_status = EXCLUDED.icp_status',
                'was_prospect = EXCLUDED.was_prospect',
                'ol = EXCLUDED.ol',
                'paid_on_product = EXCLUDED.paid_on_product',
                'live_class = EXCLUDED.live_class',
                'lead_owner = EXCLUDED.lead_owner',
                'number_of_dials_prospect = EXCLUDED.number_of_dials_prospect',
                'number_of_dials = EXCLUDED.number_of_dials',
                'number_of_dials_attempted = EXCLUDED.number_of_dials_attempted',
                'number_of_connects = EXCLUDED.number_of_connects',
                'paid_on_product_and_organic = EXCLUDED.paid_on_product_and_organic',
                'docs = EXCLUDED.docs',
                'responded_for_want_a_call = EXCLUDED.responded_for_want_a_call',
                'lead_quality = EXCLUDED.lead_quality',
                'rfd_date = EXCLUDED.rfd_date',
                'marks_obtained = EXCLUDED.marks_obtained',
                'test_date = EXCLUDED.test_date',
                'total_mcqs_attempted = EXCLUDED.total_mcqs_attempted',
                'utm_source = EXCLUDED.utm_source',
                'utm_medium = EXCLUDED.utm_medium',
                'utm_campaign = EXCLUDED.utm_campaign',
                'source = EXCLUDED.source',
                'lead_last_call_status = EXCLUDED.lead_last_call_status;',
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
            )
        )
    pg_conn.commit()


dag = DAG(
    'Growth_Dashboard_DAG_V3',
    default_args=default_args,
    description='An Analytics Reporting Layer DAG for Growth Dashboard',
    schedule_interval='15 * * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS growth_dashboard_v3 (
            id serial,
            email varchar(256),
            course_timeline_flow varchar(256), 
            cum_created_at DATE,
            date_joined DATE,
            cutfm_created_at DATE,
            prospect_date DATE,
            course_id int,
            created_at DATE,
            churned_date DATE,
            salary varchar(512),
            why_do_you_want_to_join varchar(512),
            degree varchar(512),
            twelfth_marks varchar(512),
            graduation_year varchar(512),
            life_status varchar(512),
            prospect_stage varchar(512),
            icp_status varchar(512),
            was_prospect varchar(512),
            ol boolean,
            paid_on_product boolean,
            live_class boolean,
            lead_owner varchar(512),
            number_of_dials_prospect int,
            number_of_dials int,
            number_of_dials_attempted int,
            number_of_connects int,
            paid_on_product_and_organic varchar(256),
            docs boolean,
            responded_for_want_a_call varchar(512),
            lead_quality varchar(256),
            rfd_date DATE,
            marks_obtained int,
            test_date DATE,
            total_mcqs_attempted int,
            utm_source varchar(256),
            utm_medium varchar(256),
            utm_campaign varchar(256),
            source varchar(256),
            lead_last_call_status varchar(256)
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_result_db',
    sql='''with update_users as (
    select distinct 
        email
    from (
        select distinct
            email_address as email
        from lsq_leads_x_activities
        where 
            now() - modified_on <= interval '3' hour
    
        union all
    
        select distinct 
            email
        from users_info
        where 
            modified_on >= current_date - interval '1' day
    ) as a
),

test_taken as(
    select distinct 
        email,
        max(test_date) as test_date,
        max(marks_obtained) as marks_obtained,
        max(total_mcqs_opened) as total_mcqs_opened,
        max(total_mcqs_attempted) as total_mcqs_attempted,
        max(total_mcqs_correct) as total_mcqs_correct
    from (
        select distinct 
            users_info.email,
            date(assessment_started_at) as test_date,
            max(marks_obtained) as marks_obtained,
            count(distinct aqum.mcq_id) filter (where aqum.id is not null) as total_mcqs_opened,
            count(distinct aqum.mcq_id) filter (where aqum.option_marked_at is not null) as total_mcqs_attempted,
            count(distinct aqum.mcq_id) filter (where aqum.marked_choice = aqum.correct_choice) as total_mcqs_correct
        from
            assessments a 
        join courses c 
            on c.course_id = a.course_id 
            and c.course_id in (800, 803, 818,819,820,821,822,823,824,825,826,1040)
        left join course_user_mapping as cum 
            on cum.course_id = c.course_id
        left join assessment_question_user_mapping aqum 
            on aqum.assessment_id = a.assessment_id 
            and aqum.course_user_mapping_id = cum.course_user_mapping_id 
        left join users_info 
            on users_info.user_id = aqum.user_id
        where 
            users_info.email not like ('%@newtonschool.co%')
        group by 1,2
    ) as a
    group by 1
    order by 3 desc
),

details as (
    select distinct 
        email,
        course_timeline_flow,
        cum_created_at,
        date_joined,
        cutfm_created_at,
        course_id,
        utm_source,
        utm_medium,
        utm_campaign,
        max("salary") as "salary",
        max("why_do_you_want_to_join") as "why_do_you_want_to_join",
        max("degree") as "degree",
        max("12th") as "12th",
        max("Graduation Year (College passing out year)") as "Graduation Year",
        max("What are you doing currently?") as life_status,
        max(conviction) as conviction
    from (
        select distinct
            email,
            course_timeline_flow,
            course_id,
            cum_created_at,
            date_joined,
            cutfm_created_at,
            utm_source,
            utm_medium,
            utm_campaign,
            case when question_text = 'What are you doing currently?' then apply_form_response end as "What are you doing currently?",
            case when question_text = 'Graduation Year (College passing out year)' then apply_form_response end as "Graduation Year (College passing out year)",
            case when question_text in ('Designation','How much salary do you get in a month currently?','What is your total yearly salary package? (LPA - Lakh Per Annum)') then apply_form_response end as "salary",
            case when question_text in ('Please mention your Work Experience in years') then apply_form_response end as "Work_ex",
            case when question_text in ('Current Location (City)') then apply_form_response end as "current_location",
            case when question_text in ('Why do you want to join the Data Science course?') then apply_form_response end as "why_do_you_want_to_join",
            case when question_text in ('What degree did you graduate in?') then apply_form_response end as "degree",
            case when question_text in ('12th Passing Marks (in Percentage)') then apply_form_response end as "12th",
            case when question_text in ('How sure are you about learning Data Science?') then apply_form_response end as conviction
        from (
            select distinct 
                users_info.email,
                apply_forms_and_questions.question_text,
                response as apply_form_response,
                Case when course_timeline_flow = 6 THEN 'New Design Experiment Timeline with Enhanced Urgency'
                    when course_timeline_flow = 7 THEN 'New Design Experiment Timeline with Counselling Call Urgency'
                    when course_timeline_flow = 8 THEN 'New Design Experiment Timeline with Entrance Exam'
                    when course_timeline_flow = 5 THEN 'New Design Experiment Switched Funnel (payment before eaf) Timeline'
                    when course_timeline_flow = 4 THEN 'New Design Experiment Timeline'
                    when course_timeline_flow = 3 THEN 'Backend Driven Timeline'
                    when course_timeline_flow = 9 THEN 'Product Inbound'
                    when course_timeline_flow = 10 THEN 'One Video'
                    when course_timeline_flow = 11 THEN 'Business Inbound'
                    when course_timeline_flow = 12 THEN 'New Design Experiment Timeline with Master Class Banner'
                    end as course_timeline_flow,
                courses.course_id,
                users_info.utm_source,
                users_info.utm_medium,
                users_info.utm_campaign,
                date(course_user_mapping.created_at) as cum_created_at,
                date(course_user_timeline_flow_mapping.created_at) as cutfm_created_at,
                date(users_info.date_joined) as date_joined
            from (select * from users_info where email in (select email from update_users)) as users_info
            left join course_user_timeline_flow_mapping 
                on course_user_timeline_flow_mapping.user_id = users_info.user_id 
                and course_user_timeline_flow_mapping.course_id in (786,759,800,818,819,820,821,822,823,824,825,826,1040)
            left join apply_form_course_user_question_mapping 
                on apply_form_course_user_question_mapping.user_id = course_user_timeline_flow_mapping.user_id 
                and apply_form_course_user_question_mapping.course_id = course_user_timeline_flow_mapping.course_id
            left join apply_forms_and_questions 
                on apply_forms_and_questions.apply_form_question_id = apply_form_course_user_question_mapping.apply_form_question_id
            left join course_user_mapping 
                on course_user_mapping.user_id = course_user_timeline_flow_mapping.user_id 
                and course_user_timeline_flow_mapping.course_id = course_user_mapping.course_id
            left join courses 
                on courses.course_id = course_user_timeline_flow_mapping.course_id 
                and courses.course_id in (786,759,800,818,819,820,821,822,823,824,825,826,1040)
            where courses.course_id in (786,759,800,818,819,820,821,822,823,824,825,826,1040)
            order by 1
        ) as a
    ) as b
    group by 1,2,3,4,5,6,7,8,9
),

user_milestones as (
    select
        email_address as email,
        max(case when lower(event_name) = 'log phone call' and lower(mx_custom_1) = 'prospect' then 1 else 0 end) as prospect_flag,
        min(case when lower(event_name) = 'log phone call' and lower(mx_custom_1) = 'prospect' then date(modified_on) end) as prospect_date,
        max(case when lower(event) = 'Document / Payment Tracking' and lower(mx_custom_1) = 'documents collected' then 1 else 0 end) as docs_collected_flag,
        max(case when lower(event) = 'sent offer letter' then 1 else 0 end) as offer_letter_flag,
        min(case when lower(event) = 'sent offer letter' then date(modified_on) end) as offer_letter_date,
        max(case when lower(event) = 'paid on product' and lower(mx_custom_4) = 'admission_process_booking_fee' then 1 else 0 end) as paid_on_product_flag,
        min(case when lower(event) = 'paid on product' and lower(mx_custom_4) = 'admission_process_booking_fee' then date(modified_on) end) as paid_on_product_date
    from lsq_leads_x_activities 
    where email_address in (select distinct email from update_users)
    group by 1
    
),

            
user_level as(
    select
        distinct 
        details.course_timeline_flow,
        details.cum_created_at,
        details.date_joined,
        details.cutfm_created_at,
        details.course_id,
        details.utm_source,
        details.utm_medium,
        details.utm_campaign,
        details."salary",
        details."why_do_you_want_to_join",
        details."degree",
        details."12th",
        details."Graduation Year",
        details.life_status,
        details.conviction,
        l.prospect_stage,
        l.email_address as email,
        case 
        when "salary" in ('Rs 25000 - Rs 30000 per month','Rs 30000 - Rs 40000 per month','Rs 40000 - Rs 50000 per month','Rs 50000 - Rs 75000 per month','Rs 75000 - Rs 100000 per month','More than 100000','3 LPA - 4.99 LPA','5 LPA or more') then 'ICP'
        when "salary" in ('Rs 20000 - Rs 30000 per month','Rs 10000 - Rs 20000 per month','Rs 20000 - Rs 24999 per month','2 LPA - 2.99 LPA','Below 2 LPA','Less than 3LPA') then 'Close to ICP'
        when "salary" in ('I am not earning right now','Not Earning') then 'Not ICP' end as icp_status,
        case when um.prospect_flag =1 then 'Yes' else null end as was_prospect,
        prospect_date,
        um.offer_letter_flag ol,
        um.offer_letter_date as offer_letter_date,
        um.paid_on_product_flag as paid_on_product,
        um.paid_on_product_date as paid_on_product_date,
        case when mid_funnel_buckets like ('%Live Class%') then 1 else 0 end as live_class,
        l.lead_owner,
        l.lead_created_on,
        l.mx_priority_status,
        um.docs_collected_flag as docs_collected,
        date(l.mx_rfd_date) as rfd_date,
        l.lead_last_call_status
    from (
        select *
        from lsq_leads_x_activities
        where email_address in (select email from update_users)
    ) as l
    left join details on l.email_address = details.email
    left join user_milestones um on l.email_address = um.email
),
            
            prospect_churned as(
            select
            distinct email_address,
            min(date(modified_on)) as first_connect
            from user_level
            right join lsq_leads_x_activities on lsq_leads_x_activities.email_address = user_level.email
            where lsq_leads_x_activities.lead_created_on is not null and lsq_leads_x_activities.lead_owner not in ('System','Jai Sharma','Praduman Goyal')
            and date_trunc('month',date(modified_on)) >= date_trunc('month',now()) - interval '3 month'
            and ((event in ('Outbound Phone Call Activity') and call_type = 'Answered') or (event in ('Log Phone Call') and mx_custom_1 not in ('CNC','CBL'))) and event in ('Outbound Phone Call Activity','Log Phone Call')
            group by 1
            order by email_address
            ),
            responded as(
            select
            distinct lsq_leads_x_activities.email_address,
            mx_custom_2 as responded_for_want_a_call
            from lsq_leads_x_activities
            where event = 'Responded for want a call'
            ),
            rejected_churned as(
            select
            distinct email_address,
            min(date(modified_on)) as first_connect
            from lsq_leads_x_activities
            where prospect_stage in ('Rejected') and current_stage = 'Rejected' and event = 'StageChange'
            group by 1
            order by 1,2
            ),
            churned_date as(
            select * from prospect_churned
            union all
            select * from rejected_churned
            ),
            churned_date_final as(
            select
            distinct email_address,
            min(first_connect) as churned_date
            from churned_date
            group by 1
            ),
            open_prospect_leads as(
            select
            distinct email_address,
            lsq_leads_x_activities.lead_owner,
            count(distinct activity_id) filter (where prospect_stage = 'Prospect' and event in ('Outbound Phone Call Activity','Log Phone Call')) as number_of_dials_prospect,
            count(distinct activity_id) filter (where event in ('Outbound Phone Call Activity','Log Phone Call')) as number_of_dials_attempted,
            count(distinct activity_id) filter (where ((event in ('Outbound Phone Call Activity') and call_type = 'NotAnswered') or (event in ('Log Phone Call') and mx_custom_1 in ('CNC'))) and event in ('Outbound Phone Call Activity','Log Phone Call')) as number_of_dials,
            count(distinct activity_id) filter (where ((event in ('Outbound Phone Call Activity') and call_type = 'Answered') or (event in ('Log Phone Call') and mx_custom_1 not in ('CNC'))) and event in ('Outbound Phone Call Activity','Log Phone Call')) as number_of_connects
            from details
            right join lsq_leads_x_activities on lsq_leads_x_activities.email_address = details.email
            where lead_created_on is not null and lead_owner not in ('System','Jai Sharma','Praduman Goyal')
            group by 1,2
            order by email_address
            )           
select distinct 
    user_level.email,
    course_timeline_flow,
    cum_created_at,
    date_joined,
    cutfm_created_at,
    prospect_date,
    course_id,
    date(lead_created_on) as created_at,
    case when max(prospect_stage) in ('Lead','Could Not Connect','Call Back Later') then null else min(churned_date_final.churned_date) end as churned_date,
    max("salary") as salary,
    max("why_do_you_want_to_join") as why_do_you_want_to_join,
    max("degree") as degree,
    max("12th") as twelfth_marks,
    max("Graduation Year") as graduation_year,
    max(life_status) as life_status,
    max(prospect_stage) as prospect_stage,
    max(icp_status) as icp_status,
    max(was_prospect) as was_prospect,
    case when max(ol) = 0 then false else true end as ol,
    case when max(paid_on_product) = 0 then false else true end as paid_on_product,
    case when max(live_class) = 0 then false else true end as live_class,
    user_level.lead_owner,
    max(number_of_dials_prospect) as number_of_dials_prospect,
    max(number_of_dials) as number_of_dials,
    max(number_of_dials_attempted) as number_of_dials_attempted,
    max(number_of_connects) as number_of_connects,
    case
    when lower(user_level.mx_priority_status) like ('%organic%') then 'Organic'
    when lower(user_level.mx_priority_status) like ('%reapplied%') then user_level.mx_priority_status
    when max(paid_on_product) > 0 and lower(user_level.mx_priority_status) not like ('%organic%') then 'Paid on Product'
    else null end as paid_on_product_and_organic,
    case when max(docs_collected) = 0 then false else true end as docs,
    max(responded.responded_for_want_a_call) as responded_for_want_a_call,
    case
    when max(conviction) in ('Very sure - I want to learn Data Science course') and max("why_do_you_want_to_join") in ('I want to learn Data science and then get a job in the field') then 'High'
    when max(conviction) in ('Less sure - I''m still researching about it') and max("why_do_you_want_to_join") in ('I want to learn Data science and then get a job in the field') then 'Medium'
    when max(conviction) in ('Very sure - I want to learn Data Science course') and max("why_do_you_want_to_join") in ('Need an IT job immediately - I''m NOT looking for a course','Just upgrading my skills - I''m NOT looking to change my current job') then 'Medium'
    when max(conviction) in ('Very sure - I want to learn Data Science course') and max("why_do_you_want_to_join") in ('Other reason') then 'Low'
    when max(conviction) in ('Less sure - I''m still researching about it') and max("why_do_you_want_to_join") in ('Need an IT job immediately - I''m NOT looking for a course','Just upgrading my skills - I''m NOT looking to change my current job','Other reason') then 'Low'
    when max(conviction) in ('No - I don''t plan to do a Data Science Course right now') then 'Low' end as lead_quality,
    rfd_date,
    test_taken.marks_obtained,
    test_taken.test_date,
    test_taken.total_mcqs_attempted,
    user_level.utm_source,
    utm_medium,
    utm_campaign,
    case when source_mapping.source is null then 'Organic' else source_mapping.source end as source,
    lead_last_call_status 
from user_level
left join churned_date_final on churned_date_final.email_address = user_level.email
left join open_prospect_leads on open_prospect_leads.email_address = user_level.email
left join responded on responded.email_address = user_level.email
left join test_taken on test_taken.email = user_level.email
left join source_mapping on source_mapping.utm_source = user_level.utm_source 
group by 1,2,3,4,5,6,7,8,user_level.lead_owner,user_level.mx_priority_status,rfd_date,test_taken.marks_obtained,test_taken.test_date,test_taken.total_mcqs_attempted,user_level.utm_source,utm_medium,utm_campaign,source_mapping.source,lead_last_call_status
    ;
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
