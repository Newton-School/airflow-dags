from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 16),
}

dag = DAG(
    'ds_inbound_form_response',
    default_args=default_args,
    description='Load inbound form data with eligibility & sign-up analysis',
    schedule_interval='7 */4 * * *',  # every 4 hours
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''
    CREATE TABLE IF NOT EXISTS ds_inbound_form_filled (
        form_id INT PRIMARY KEY,
        user_id INT,
        full_name VARCHAR(512),
        email VARCHAR(256),
        phone_number VARCHAR(20),
        response_type VARCHAR(256),
        from_source VARCHAR(256),
        form_created_at TIMESTAMP,
        current_status VARCHAR(512),
        graduation_year VARCHAR(512),
        highest_qualification VARCHAR(256),
        graduation_degree VARCHAR(512),
        current_job_role VARCHAR(512),
        course_type_interested_in VARCHAR(512),
        is_inquiry_for_data_science_certification VARCHAR(512),
        user_date_joined TIMESTAMP,
        utm_source VARCHAR(512),
        utm_medium VARCHAR(512),
        utm_campaign VARCHAR(512),
        inbound_key VARCHAR(256),
        first_action VARCHAR(256),
        eligible BOOLEAN
    );
    ''',
    dag=dag
)

load_data = PostgresOperator(
    task_id='load_data',
    postgres_conn_id='postgres_read_replica',
    sql='''
    WITH RankedResponses AS (
        SELECT 
            m.id,
            m.response_type,
            m.created_at,
            m.response_json->>'email' AS email,
            m.response_json->>'full_name' AS full_name,
            m.response_json->>'phone_number' AS phone_number,
            m.response_json->>'current_status' AS current_status,
            m.response_json->>'graduation_year' AS graduation_year,
            m.response_json->>'highest_qualification' AS highest_qualification,
            m.response_json->>'degree' AS graduation_degree,
            m.response_json->>'current_role' AS current_job_role,
            m.response_json->>'course_type_interested_in' AS course_type_interested_in,
            m.response_json->>'is_inquiry_for_data_science_certification' AS is_inquiry_for_data_science_certification,
            m.response_json->>'utm_source' AS utm_source,
            m.response_json->>'utm_medium' AS utm_medium,
            m.response_json->>'utm_campaign' AS utm_campaign,
            m.response_json->>'from' AS from_source,
            CASE 
                WHEN m.response_type = 'PUBLIC_WEBSITE_HOME_REQUEST_CALLBACK_FORM' AND m.response_json->>'course_type_interested_in' = 'ds' THEN 'RCB_HP'
                WHEN m.response_type = 'PUBLIC_WEBSITE_DS_HOME_REQUEST_CALLBACK_FORM' AND m.response_json->>'from' = 'request_callback' THEN 'RCB_DS'
                WHEN m.response_type = 'PUBLIC_WEBSITE_DS_HOME_REQUEST_CALLBACK_FORM' AND m.response_json->>'from' = 'ad_landing_page' THEN 'RCB_ALP'
                WHEN m.response_type = 'PUBLIC_WEBSITE_DS_HOME_REQUEST_CALLBACK_FORM' AND m.response_json->>'from' = 'social_media_rcb' THEN 'RCB_SM'
                WHEN m.response_type = 'PUBLIC_WEBSITE_DS_HOME_REQUEST_CALLBACK_FORM' AND m.response_json->>'from' = 'connect_with_alumni' THEN 'RCB_AL'
                WHEN m.response_type = 'PUBLIC_WEBSITE_CONTACT_FORM' AND m.response_json->>'is_inquiry_for_data_science_certification' = 'professional_certification_in_ds_ai' THEN 'CF'
                WHEN m.response_type = 'PUBLIC_WEBSITE_CONTACT_FORM' AND m.response_json->>'is_inquiry_for_data_science_certification' = 'btech_in_cs_ai_from_nst' THEN 'CF_NST'
                WHEN m.response_type = 'PUBLIC_WEBSITE_CHATBOT' THEN 'CB'
                WHEN m.response_type = 'PUBLIC_WEBSITE_DS_HOME_REQUEST_CALLBACK_FORM' AND m.response_json->>'from' = 'download_brochure' THEN 'DB'
                WHEN m.response_type = 'DS_TIMELINE_REQUEST_CALLBACK_FORM' THEN 'DS_RCB'
                WHEN m.response_type = 'PUBLIC_WEBSITE_HOME_REQUEST_CALLBACK_FORM' AND m.response_json->>'from' = 'download_brochure_v2' THEN 'DB_DS'
                WHEN m.response_type = 'PUBLIC_WEBSITE_HOME_REQUEST_CALLBACK_FORM' AND m.response_json->>'from' = 'perf_request_a_callback' THEN 'RCB_Perf'
                WHEN m.response_type = 'PUBLIC_FULLSTACK_WEBSITE_HOME_REQUEST_CALLBACK_FORM' AND m.response_json->>'from' = 'download_brochure' THEN 'DB_ASD'
                WHEN m.response_type = 'PUBLIC_WEBSITE_DS_HOME_REQUEST_CALLBACK_FORM' AND m.response_json->>'from' = 'timer_pop_up_ds_page_v2' THEN 'Tipo_DS'
                WHEN m.response_type = 'PUBLIC_FULLSTACK_WEBSITE_HOME_REQUEST_CALLBACK_FORM' AND m.response_json->>'from' = 'timer_pop_up_asd_page' THEN 'TiPo_FSD'
                WHEN m.response_type = 'MASTER_CLASS_REQUEST_CALLBACK_FORM' THEN 'MC_RCB'
                WHEN m.response_type = 'PUBLIC_WEBSITE_DS_HOME_REQUEST_CALLBACK_FORM' AND m.response_json->>'from' = 'request_callback_v2' THEN 'RCB_ASD'
                WHEN m.response_type = 'PUBLIC_FULLSTACK_WEBSITE_HOME_REQUEST_CALLBACK_FORM' AND m.response_json->>'from' = 'request_callback' THEN 'RCB_FSD'
                WHEN m.response_type = 'PUBLIC_WEBSITE_DS_HOME_REQUEST_CALLBACK_FORM' AND m.response_json->>'from' = 'timer_pop_up_ds_page' THEN 'TIPO_DS_V1'
                WHEN m.response_type = 'SAT_REQUEST_CALLBACK_FORM' THEN 'RCB_SAT'
                WHEN m.response_type = 'PUBLIC_WEBSITE_HOME_REQUEST_CALLBACK_FORM' AND m.response_json->>'from' = 'about_us_timer_pop_up' THEN 'TiPO_ABOUT_US'
                WHEN m.response_type = 'PUBLIC_FULLSTACK_WEBSITE_HOME_REQUEST_CALLBACK_FORM' AND m.response_json->>'from' = 'hp_download_brochure' THEN 'DB_HP'
                WHEN m.response_type = 'PUBLIC_WEBSITE_HOME_REQUEST_CALLBACK_FORM' AND m.response_json->>'from' = 'about_us_bottom_banner' THEN 'RCB_ABOUT_US'
                WHEN m.response_type = 'PUBLIC_WEBSITE_HOME_REQUEST_CALLBACK_FORM' AND m.response_json->>'from' = 'tnb_request_a_callback' THEN 'RCB_TNB'
                WHEN m.response_type = 'PUBLIC_WEBSITE_HOME_REQUEST_CALLBACK_FORM' AND m.response_json->>'from' = 'alumni_page_bottom_banner_rcb' THEN 'RCB_ALUMNI_PAGE'
                WHEN m.response_type = 'PUBLIC_WEBSITE_DS_HOME_REQUEST_CALLBACK_FORM' AND m.response_json->>'from' = 'timer_pop_up_ds_page_v2_new_perf' THEN 'Tipo_NEW_PERF'
                WHEN m.response_type = 'PUBLIC_WEBSITE_DS_HOME_REQUEST_CALLBACK_FORM' AND m.response_json->>'from' = 'new_perf_request_a_callback' THEN 'RCB_NEW_PERF'
                WHEN m.response_type = 'PUBLIC_WEBSITE_DS_HOME_REQUEST_CALLBACK_FORM' AND m.response_json->>'from' = 'new_perf_pr' THEN 'PR_PERF'
                WHEN m.response_type = 'PUBLIC_WEBSITE_DS_HOME_REQUEST_CALLBACK_FORM' AND m.response_json->>'from' = 'New_perf_request_a_callback' THEN 'RCB_NEW_PERF'
                WHEN m.response_type = 'PUBLIC_WEBSITE_DS_HOME_REQUEST_CALLBACK_FORM' AND m.response_json->>'from' = 'perf_download_brochure_v2' THEN 'DB_PERF'
                WHEN m.response_type = 'PUBLIC_FULLSTACK_WEBSITE_HOME_REQUEST_CALLBACK_FORM' AND m.response_json->>'from' = 'download_brochure_new_perf' THEN 'DB_PERF'
                WHEN m.response_type = 'PUBLIC_WEBSITE_DS_HOME_REQUEST_CALLBACK_FORM' AND m.response_json->>'from' = 'WA_bio_request_callback_v2' THEN 'WA_BIO'
                ELSE 'UNKNOWN'
            END AS inbound_key
        FROM marketing_genericformresponse m
        WHERE m.response_type NOT IN (
            'ERP_EXPECTED_MARKS_FORM',
            'NST_REQUEST_ONLINE_COUNSELLING_SLOT',
            'ERP_PREVIOUS_YEAR_EXAM_FORM',
            'HEADSTART_COURSE_SELECTOR',
            'PUBLIC_WEBSITE_CHATBOT'
        )
    ),
    UserSignIn AS (
        SELECT 
            au.email,
            uup.phone,
            au.id,
            CONCAT(au.first_name, ' ', au.last_name) AS Name,
            au.date_joined,
            eup.graduation_year AS PGY,
            uup.utm_param_json->>'utm_source' AS utm_source,
            uup.utm_param_json->>'utm_medium' AS utm_medium,
            uup.utm_param_json->>'utm_campaign' AS utm_campaign
        FROM auth_user au
        JOIN users_userprofile uup ON uup.user_id = au.id
        LEFT JOIN users_extendeduserprofile eup ON eup.user_id = au.id
    ),
    FinalResult AS (
        SELECT
            r.id AS form_id,
            u.id AS user_id,
            r.full_name,
            r.email,
            r.phone_number,
            r.response_type,
            r.from_source,
            r.created_at AS form_created_at,
            r.current_status,
            r.graduation_year,
            r.highest_qualification,
            r.graduation_degree,
            r.current_job_role,
            r.course_type_interested_in,
            r.is_inquiry_for_data_science_certification,
            u.date_joined AS user_date_joined,
            r.utm_source,
            r.utm_medium,
            r.utm_campaign,
            r.inbound_key,
            CASE 
                WHEN u.date_joined < r.created_at THEN 'Signed In First'
                ELSE 'Filled Form First'
            END AS first_action,
            CASE 
                WHEN r.graduation_year ~ '^\d+$' 
                    AND (CAST(r.graduation_year AS INT) BETWEEN 2017 AND 2024)
                    AND r.highest_qualification NOT IN ('12th','diploma') 
                    AND r.highest_qualification IS NOT NULL 
                THEN TRUE 
                ELSE FALSE 
            END AS eligible
        FROM RankedResponses r
        LEFT JOIN UserSignIn u ON r.email = u.email
        WHERE (
            CASE 
                WHEN u.date_joined < r.created_at THEN 'Signed In First'
                ELSE 'Filled Form First'
            END
        ) = 'Filled Form First'
    )

    INSERT INTO ds_inbound_form_filled (
        form_id,
        user_id,
        full_name,
        email,
        phone_number,
        response_type,
        from_source,
        form_created_at,
        current_status,
        graduation_year,
        highest_qualification,
        graduation_degree,
        current_job_role,
        course_type_interested_in,
        is_inquiry_for_data_science_certification,
        user_date_joined,
        utm_source,
        utm_medium,
        utm_campaign,
        inbound_key,
        first_action,
        eligible
    )
    SELECT 
        form_id,
        user_id,
        full_name,
        email,
        phone_number,
        response_type,
        from_source,
        form_created_at,
        current_status,
        graduation_year,
        highest_qualification,
        graduation_degree,
        current_job_role,
        course_type_interested_in,
        is_inquiry_for_data_science_certification,
        user_date_joined,
        utm_source,
        utm_medium,
        utm_campaign,
        inbound_key,
        first_action,
        eligible
    FROM FinalResult
    ON CONFLICT (form_id) DO NOTHING;
    ''',
    dag=dag
)

create_table >> load_data

