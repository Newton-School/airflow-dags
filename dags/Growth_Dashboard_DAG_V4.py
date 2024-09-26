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
            'INSERT INTO growth_dashboard_v4 (prospect_id, email, lead_created_on, latest_stage,' 
            'latest_stage_timestamp, prospect_stage,  lead_assigned_flag, first_lead_assigned_timestamp,'
            'prospect_flag, first_prospect_timestamp, test_taken_flag, test_taken_timestamp, test_cleared_flag,' 
            'test_cleared_timestamp, session_done_flag, first_session_done_timestamp, docs_collected_flag,' 
            'docs_collected_timestamp, lead_owner, first_call_timestamp, last_call_timestamp, dials, connects,'
            'connects_gt_3min, duration, churn_flag, churn_timestamp, true_churn_flag, true_churn_timestamp,'
            'course_timeline_flow, course_id, cum_created_at, date_joined, cutfm_created_at, utm_source, utm_medium,' 
            'utm_campaign, utm_referer, course_slug, marketing_slug, utm_hash, incoming_course_structure_slug, latest_utm_source,'
            'latest_utm_campaign, latest_utm_medium, twelfth_marks, graduation_year, degree, life_status, work_ex, salary,' 
            'current_location, reason_to_join, conviction, mx_identifer, mx_lead_inherent_intent, mx_lead_quality_grade, icp_status)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);',
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
            )
        )
    pg_conn.commit()


dag = DAG(
    'Growth_Dashboard_DAG_V4',
    default_args=default_args,
    description='DAG for collating user metrics/milestones for growth & sales',
    schedule_interval=None,
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS growth_dashboard_v4 (
            id serial,
            prospect_id varchar(256),
            email varchar(256),
            lead_created_on timestamp,
            latest_stage varchar(512),
            latest_stage_timestamp timestamp,
            prospect_stage varchar(512),
            lead_assigned_flag int,
            first_lead_assigned_timestamp timestamp,
            prospect_flag int,
            first_prospect_timestamp timestamp,
            test_taken_flag int,
            test_taken_timestamp timestamp,
            test_cleared_flag int,
            test_cleared_timestamp timestamp,
            session_done_flag int,
            first_session_done_timestamp timestamp,
            docs_collected_flag int,
            docs_collected_timestamp timestamp,
            lead_owner varchar(512),
            first_call_timestamp timestamp,
            last_call_timestamp timestamp,
            dials int,
            connects int,
            connects_gt_3min int,
            duration int,
            churn_flag int,
            churn_timestamp timestamp,
            true_churn_flag int,
            true_churn_timestamp timestamp,
            course_timeline_flow varchar(256),
            course_id int,
            cum_created_at date,
            date_joined date,
            cutfm_created_at date,
            utm_source varchar(512),
            utm_medium varchar(512),
            utm_campaign varchar(512),
            utm_referer varchar(512),
            course_slug varchar(512),
            marketing_slug varchar(512),
            utm_hash varchar(512),
            incoming_course_structure_slug varchar(512),
            latest_utm_source varchar(512),
            latest_utm_campaign varchar(512),
            latest_utm_medium varchar(512),
            twelfth_marks varchar(512),
            graduation_year varchar(512),
            degree varchar(512),
            work_ex varchar(512),
            salary varchar(512),
            current_location varchar(512),
            reason_to_join varchar(512),
            mx_identifer varchar(256),
            mx_lead_inherent_intent varchar(256),
            mx_lead_quality_grade varchar(256),
            icp_status boolean
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_result_db',
    sql='''
        with prospect_list as (
            select distinct 
                a.prospect_id,
                email_address,
                prospect_stage,
                lead_created_on,
                latest_timestamp,
                mx_identifer,
                mx_lead_inherent_intent,
                mx_lead_quality_grade
            from (
                select distinct
                    prospect_id,
                    email_address,
                    prospect_stage,
                    lead_created_on,
                    modified_on as latest_timestamp
                from (
                    select 
                        prospect_id,
                        email_address,
                        prospect_stage,
                        lead_created_on,
                        modified_on,
                        row_number() over (partition by prospect_id order by modified_on desc) as rn
                    from lsq_leads_x_activities
                    -- where modified_on >= now() - interval '12' hour
                ) a
                where rn = 1
            ) as a
            left join (
                select 
                    prospect_id,
                    min(case when mx_lead_inherent_intent is not null then mx_lead_inherent_intent end) as mx_lead_inherent_intent,
                    min(case when mx_identifer is not null then mx_identifer end) as mx_identifer,
                    min(case when mx_lead_quality_grade is not null then mx_lead_quality_grade end) as mx_lead_quality_grade
                from lsq_leads_x_activities
                group by 1
            ) as b on a.prospect_id = b.prospect_id
        ),

        latest_stage as (
            select 
                a.prospect_id as prospect_id,
                a.email_address as email,
                prospect_stage,
                case when time_stage is null and time_log is not null then mx_custom_1
                    when time_log is null and time_stage is not null then current_stage
                    when time_log is null and time_stage is null then prospect_stage
                    when time_log >= time_stage then mx_custom_1
                    when time_stage >= time_log then current_stage
                    else prospect_stage
                    end as latest_stage,
                coalesce(greatest(time_log, time_stage), latest_timestamp) as latest_stage_timestamp
            from (
                select distinct 
                    prospect_id,
                    email_address,
                    prospect_stage,
                    latest_timestamp
                from prospect_list
            ) as a
            left join (
                select 
                    prospect_id,
                    current_stage,
                    time_stage
                from (
                    select 
                        prospect_id,
                        current_stage,
                        modified_on as time_stage,
                        row_number() over(partition by prospect_id order by modified_on desc) as rn_stage
                    from lsq_leads_x_activities
                    where 
                        current_stage is not null
                        -- and modified_on >= now() - interval '12' hour
                ) a 
                where rn_stage = 1
            ) as b on a.prospect_id = b.prospect_id
            left join (
                select 
                    prospect_id,
                    mx_custom_1,
                    time_log
                from (
                    select 
                        prospect_id,
                        mx_custom_1,
                        modified_on as time_log,
                        row_number() over(partition by prospect_id order by modified_on desc) as rn_log
                    from lsq_leads_x_activities
                    where 
                        event = 'Log Phone Call'
                        -- and modified_on >= now() - interval '12' hour
                ) as b 
                where rn_log = 1
            ) as c
            on a.prospect_id = c.prospect_id
        ),


        call_data as (
            select 
                prospect_id,
                email_address,
                case when call_type = 'Answered' and caller is not null then caller
                    when call_type in ('NotAnswered', 'Call Failure') and call_notes is not null then call_notes
                    else lead_owner end as BDE,
                lead_owner,
                modified_on as call_timestamp,
                activity_id,
                event,
                duration,
                call_type,
                row_number() over(partition by prospect_id order by modified_on asc ) as rn_asc,
                row_number() over(partition by prospect_id order by modified_on desc) as rn_desc
            from lsq_leads_x_activities
            where 
                event in ('Outbound Phone Call Activity', 'Inbound Phone Call Activity')
                and prospect_id in (select prospect_id from prospect_list)
                -- and modified_on >= now() - interval '12' hour
        ),


        -- call_data_summarized as (
        --     select 
        --         a.prospect_id as prospect_id,
        --         email_address,
        --         BDE,
        --         max(b.lead_owner) as lead_owner,

        --         min(call_timestamp) as first_call_timestamp_bde,
        --         max(call_timestamp) as last_call_timestamp_bde,
        --         count(distinct activity_id) as calls_attempted_bde,
        --         count(distinct case when call_type = 'Answered' then activity_id end) as calls_answered_bde,
        --         count(distinct case when call_type = 'Answered' and duration >180 then activity_id end) as calls_answered_gt3min_bde,
        --         sum(case when call_type = 'Answered' then duration else 0 end) as duration_bde,

        --         max(first_call_timestamp_overall) as first_call_timestamp_overall,
        --         max(last_call_timestamp_overall) as last_call_timestamp_overall,
        --         max(sixth_call_timestamp_overall) as sixth_call_timestamp_overall,
        --         max(calls_attempted_overall) as calls_attempted_overall,
        --         max(calls_answered_overall) as calls_answered_overall,
        --         max(calls_answered_gt3min_overall) as calls_answered_gt3min_overall,
        --         max(duration_overall) as duration_overall
        --     from call_data a
        --     left join (
        --         select 
        --             prospect_id,
        --             max(case when rn_desc = 1 then lead_owner end) as lead_owner,
        --             min(call_timestamp) as first_call_timestamp_overall,
        --             max(call_timestamp) as last_call_timestamp_overall,
        --             min(case when rn_asc = 6 then call_timestamp end) as sixth_call_timestamp_overall,
        --             count(distinct activity_id) as calls_attempted_overall,
        --             count(distinct case when call_type = 'Answered' then activity_id end) as calls_answered_overall,
        --             count(distinct case when call_type = 'Answered' and duration > 180 then activity_id end) as calls_answered_gt3min_overall,
        --             sum(case when call_type = 'Answered' then duration else 0 end) as duration_overall
        --         from call_data
        --         group by 1
        --     ) b on a.prospect_id = b.prospect_id
        --     group by 1,2,3
        -- ),

        call_data_summarized as (
            select 
                prospect_id,
                max(case when rn_desc = 1 then lead_owner end) as lead_owner,
                min(call_timestamp) as first_call_timestamp,
                max(call_timestamp) as last_call_timestamp,
                min(case when rn_asc = 6 then call_timestamp end) as sixth_call_timestamp,
                count(distinct activity_id) as calls_attempted,
                count(distinct case when call_type = 'Answered' then activity_id end) as calls_answered,
                count(distinct case when call_type = 'Answered' and duration > 180 then activity_id end) as calls_answered_gt3min,
                sum(case when call_type = 'Answered' then duration else 0 end) as duration
            from call_data
            group by 1
        ),

        lead_data as (
            select 
                a.prospect_id as prospect_id,
                max(email) as email,
                max(a.lead_created_on) as lead_created_on,
                max(latest_stage) as latest_stage,
                max(latest_stage_timestamp) as latest_stage_timestamp,
                max(prospect_stage) as prospect_stage,
                max(a.mx_identifer) as mx_identifer,
                max(a.mx_lead_inherent_intent) as mx_lead_inherent_intent,
                max(a.mx_lead_quality_grade) as mx_lead_quality_grade,
                
                max(lead_assigned_flag) as lead_assigned_flag,
                max(first_lead_assigned_timestamp) as first_lead_assigned_timestamp,
                max(prospect_flag) as prospect_flag,
                max(first_prospect_timestamp) as first_prospect_timestamp,
                max(test_taken_flag) as test_taken_flag,
                max(test_taken_timestamp) as test_taken_timestamp,
                max(test_cleared_flag) as test_cleared_flag,
                max(test_cleared_timestamp) as test_cleared_timestamp,
                max(session_done_flag) as session_done_flag,
                max(first_session_done_timestamp) as first_session_done_timestamp,
                max(docs_collected_flag) as docs_collected_flag,
                max(docs_collected_timestamp) as docs_collected_timestamp,

                max(lead_owner) as lead_owner,
                max(first_call_timestamp) as first_call_timestamp,
                max(last_call_timestamp) as last_call_timestamp,
                max(calls_attempted) as dials,
                max(calls_answered) as connects,
                max(calls_answered_gt3min) as connects_gt_3min,
                max(duration) as duration,

                max(case when calls_attempted >=1 then 1 else 0 end) as churn_flag,
                max(case when calls_attempted >=1 then first_call_timestamp end) as churn_timestamp,
                max(case when calls_attempted >=6 then 1
                        when lower(latest_stage) not in ('call back later', 'lead', 'could not connect') then 1
                    else 0 end) as true_churn_flag,
                max(case when calls_attempted >=6 then sixth_call_timestamp
                    when lower(latest_stage) not in ('call back later', 'lead', 'could not connect') then latest_stage_timestamp end) as true_churn_timestamp

            from prospect_list a
            left join (
                select 
                    a.prospect_id as prospect_id,
                    max(email) as email,
                    max(a.lead_created_on) as lead_created_on,
            
                    --milestones
                    max(case when event in ('LeadAssigned') then 1 else 0 end) as lead_assigned_flag,
                    min(case when event in ('LeadAssigned') then modified_on end) as first_lead_assigned_timestamp,
            
                    max(case when event in ('Log Phone Call') and mx_custom_1 = 'Prospect' then 1 else 0 end) as prospect_flag,
                    min(case when event in ('Log Phone Call') and mx_custom_1 = 'Prospect' then modified_on end) as first_prospect_timestamp,
            
                    max(case when mx_lead_type is null and (lower(event) in ('ended entrance exam', 'entrance exam completed')) then 1 else 0 end) as test_taken_flag,
                    min(case when mx_lead_type is null and (lower(event) in ('ended entrance exam', 'entrance exam completed')) then modified_on end) as test_taken_timestamp,
            
                    max(case when mx_lead_type is null and ((lower(event) in ('ended entrance exam') and regexp_replace(mx_custom_4, '\D','','g')::numeric >= 38) or (lower(event) in ('ended entrance exam', 'entrance exam completed') and mx_entrance_exam_marks::numeric >= 38)) then 1 else 0 end) as test_cleared_flag,
                    min(case when mx_lead_type is null and ((lower(event) in ('ended entrance exam') and regexp_replace(mx_custom_4, '\D','','g')::numeric >= 38) or (lower(event) in ('ended entrance exam', 'entrance exam completed') and mx_entrance_exam_marks::numeric >= 38)) then modified_on end) as test_cleared_timestamp,
            
                    max(case when event = 'Log Phone Call' and mx_custom_1 = 'Session Done' then 1 else 0 end) as session_done_flag,
                    min(case when event = 'Log Phone Call' and mx_custom_1 = 'Session Done' then modified_on end) as first_session_done_timestamp,
            
                    max(case when event = 'StageChange' and lower(current_stage) = 'NBFC Docs Collected' then 1
                            when lower(event) = 'document / payment tracking' and lower(mx_custom_1) = 'documents collected' then 1
                            else 0 end) as docs_collected_flag,
                    min(case when event = 'StageChange' and lower(current_stage) = 'NBFC Docs Collected' then modified_on
                            when lower(event) = 'document / payment tracking' and lower(mx_custom_1) = 'documents collected' then modified_on
                            end) as docs_collected_timestamp,
            
                    max(latest_stage) as latest_stage,
                    max(latest_stage_timestamp) as latest_stage_timestamp
                
                from (
                    select 
                        prospect_id,
                        lead_created_on,
                        event,
                        mx_custom_1,
                        mx_custom_4,
                        mx_entrance_exam_marks,
                        modified_on,
                        mx_lead_type,
                        current_stage
                    from lsq_leads_x_activities
                    where 
                        prospect_id in (select prospect_id from prospect_list)
                        -- and modified_on >= now() - interval '12' hour
                ) a
                left join latest_stage b on a.prospect_id = b.prospect_id
                group by 1
            ) b on a.prospect_id = b.prospect_id
            left join call_data_summarized c on a.prospect_id = c.prospect_id
            group by 1

        ),

        user_data as(
            select distinct 
                email,
                max(course_timeline_flow) as course_timeline_flow,
                max(course_id) as course_id,
                max(cum_created_at) as cum_created_at,
                max(date_joined) as date_joined,
                max(cutfm_created_at) as cutfm_created_at,
                max(utm_source) as utm_source,
                max(utm_medium) as utm_medium,
                max(utm_campaign) as utm_campaign,
                max(utm_referer) as utm_referer,
                max(course_slug) as course_slug,
                max(marketing_slug) as marketing_slug,
                max(utm_hash) as utm_hash,
                max(incoming_course_structure_slug) as incoming_course_structure_slug,
                max(latest_utm_source) as latest_utm_source,
                max(latest_utm_campaign) as latest_utm_campaign,
                max(latest_utm_medium) as latest_utm_medium,
                max(case when question_text in ('12th Passing Marks (in Percentage)') then apply_form_response end) as "12th",
                max(case when question_text = 'Graduation Year (College passing out year)' then apply_form_response end) as "Graduation Year",
                max(case when question_text in ('What degree did you graduate in?') then apply_form_response end) as "Degree",
                max(case when question_text = 'What are you doing currently?' then apply_form_response end) as "Life Status",
                max(case when question_text in ('Please mention your Work Experience in years') then apply_form_response end) as "Work ex",
                max(case when question_text in ('Designation','How much salary do you get in a month currently?','What is your total yearly salary package? (LPA - Lakh Per Annum)') then apply_form_response end) as "Salary",
                max(case when question_text in ('Current Location (City)') then apply_form_response end) as "Current location",
                max(case when question_text in ('Why do you want to join the Data Science course?') then apply_form_response end) as "Reason to Join",
                max(case when question_text in ('How sure are you about learning Data Science?') then apply_form_response end) as "Conviction"
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
                    users_info.utm_referer,
                    users_info.course_slug,
                    users_info.marketing_slug,
                    users_info.utm_hash,
                    users_info.incoming_course_structure_slug,
                    users_info.latest_utm_source,
                    users_info.latest_utm_campaign,
                    users_info.latest_utm_medium,
                    date(course_user_mapping.created_at) as cum_created_at,
                    date(course_user_timeline_flow_mapping.created_at) as cutfm_created_at,
                    date(users_info.date_joined) as date_joined
                from users_info
                left join course_user_timeline_flow_mapping on course_user_timeline_flow_mapping.user_id = users_info.user_id and course_user_timeline_flow_mapping.course_id in (786,759,800,818,819,820,821,822,823,824,825,826,1040,1041)
                left join apply_form_course_user_question_mapping on apply_form_course_user_question_mapping.user_id = course_user_timeline_flow_mapping.user_id and apply_form_course_user_question_mapping.course_id = course_user_timeline_flow_mapping.course_id
                left join apply_forms_and_questions on apply_forms_and_questions.apply_form_question_id = apply_form_course_user_question_mapping.apply_form_question_id
                left join course_user_mapping on course_user_mapping.user_id = course_user_timeline_flow_mapping.user_id and course_user_timeline_flow_mapping.course_id = course_user_mapping.course_id
                left join courses on courses.course_id = course_user_timeline_flow_mapping.course_id and courses.course_id in (786,759,800,818,819,820,821,822,823,824,825,826,1040,1041)
                where courses.course_id in (786,759,800,818,819,820,821,822,823,824,825,826,1040,1041)
                order by 1
            ) as a
            group by 1
        )

        select distinct 
            a.prospect_id as prospect_id,
            a.email as email,
            lead_created_on,
            latest_stage,
            latest_stage_timestamp,
            prospect_stage,
                
            lead_assigned_flag,
            first_lead_assigned_timestamp,
            prospect_flag,
            first_prospect_timestamp,
            test_taken_flag,
            test_taken_timestamp,
            test_cleared_flag,
            test_cleared_timestamp,
            session_done_flag,
            first_session_done_timestamp,
            docs_collected_flag,
            docs_collected_timestamp,
            
            lead_owner,
            first_call_timestamp,
            last_call_timestamp,
            dials,
            connects,
            connects_gt_3min,
            duration,
            
            churn_flag,
            churn_timestamp,
            true_churn_flag,
            true_churn_timestamp,

            course_timeline_flow,
            course_id,
            cum_created_at,
            date_joined,
            cutfm_created_at,
            utm_source,
            utm_medium,
            utm_campaign,
            utm_referer,
            course_slug,
            marketing_slug,
            utm_hash,
            incoming_course_structure_slug,
            latest_utm_source,
            latest_utm_campaign,
            latest_utm_medium,
            "12th" as twelfth_marks,
            "Graduation Year" as graduation_year,
            "Degree" as degree,
            "Life Status" as life_status,
            "Work ex" as work_ex,
            "Salary" as salary,
            "Current location" as current_location,
            "Reason to Join" as reason_to_join,
            "Conviction" as conviction,
            mx_identifer,
            mx_lead_inherent_intent,
            mx_lead_quality_grade,
            case when mx_lead_quality_grade in ('Grade A') and lower(mx_lead_inherent_intent) in ('high', 'medium', 'low', '') then true
                when mx_lead_quality_grade in ('Grade B', 'Grade C', 'Grade D', 'Grade E', 'Grade F') and lower(mx_lead_inherent_intent) in ('high', 'medium') then true
                else false end as icp_status
        from lead_data a
        left join user_data b on a.email = b.email
        ;
        ''',
    dag=dag
)
drop_table = PostgresOperator(
    task_id='drop_table',
    postgres_conn_id='postgres_result_db',
    sql='''DROP TABLE IF EXISTS growth_dashboard_v4;
    ''',
    dag=dag
)

extract_python_data = PythonOperator(
    task_id='extract_python_data',
    python_callable=extract_data_to_nested,
    provide_context=True,
    dag=dag
)
transform_data >> drop_table >> create_table >> extract_python_data
