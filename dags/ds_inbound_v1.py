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
            'INSERT INTO temp_table ('
                'id,response_type,from_source,email,full_name,phone_number,current_status,graduation_year,'
                'highest_qualification,course_type_interested_in,is_inquiry_for_data_science_certification,'
                'form_created_at,user_date_joined,inbound_key,first_action,eligible)'
                'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);',
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
            )
        )
    pg_conn.commit()


dag = DAG(
    'DS_Inbound_DAG_v1',
    default_args=default_args,
    description='DS Inbound FTA with Sign-In form filled',
    schedule_interval='30 * * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS ds_inbound_form_filled (
            id int,
            response_type varchar(256),
            from_source varchar(256),
            email varchar(256),
            full_name varchar(512),
            phone_number bigint,
            current_status varchar(512),
            graduation_year varchar(512),
            highest_qualification varchar(256),
            course_type_interested_in varchar(512),
            is_inquiry_for_data_science_certification varchar(512),
            form_created_at TIMESTAMP,
            user_date_joined TIMESTAMP,
            inbound_key varchar(256),
            first_action varchar(256),
            eligible boolean
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''
    WITH RankedResponses AS (
    SELECT 
        marketing_genericformresponse.id,
        response_type,
        response_json->>'from' AS from_source,
        response_json->>'email' AS email,
        response_json->>'full_name' AS full_name,
        response_json->>'phone_number' AS phone_number,
        response_json->>'current_status' AS current_status,
        response_json->>'graduation_year' AS graduation_year,
        response_json->>'highest_qualification' AS highest_qualification,
        response_json->>'course_type_interested_in' AS course_type_interested_in,
        response_json->>'is_inquiry_for_data_science_certification' AS is_inquiry_for_data_science_certification,
        created_at,
        ROW_NUMBER() OVER (
            PARTITION BY response_json->>'email' 
            ORDER BY created_at ASC
        ) AS rn,
        CASE 
            WHEN response_type = 'PUBLIC_WEBSITE_HOME_REQUEST_CALLBACK_FORM' 
                 AND response_json->>'course_type_interested_in' = 'ds' THEN 'RCB_HP'
            WHEN response_type = 'PUBLIC_WEBSITE_DS_HOME_REQUEST_CALLBACK_FORM' 
                 AND response_json->>'from' = 'request_callback' THEN 'RCB_DS'
            WHEN response_type = 'PUBLIC_WEBSITE_DS_HOME_REQUEST_CALLBACK_FORM' 
                 AND response_json->>'from' = 'ad_landing_page' THEN 'RCB_ALP'
            WHEN response_type = 'PUBLIC_WEBSITE_DS_HOME_REQUEST_CALLBACK_FORM' 
                 AND response_json->>'from' = 'social_media_rcb' THEN 'RCB_SM'
            WHEN response_type = 'PUBLIC_WEBSITE_DS_HOME_REQUEST_CALLBACK_FORM' 
                 AND response_json->>'from' = 'connect_with_alumni' THEN 'RCB_AL'
            WHEN response_type = 'PUBLIC_WEBSITE_CONTACT_FORM' 
                 AND response_json->>'is_inquiry_for_data_science_certification' = 'professional_certification_in_ds_ai' THEN 'CF'
             WHEN response_type = 'PUBLIC_WEBSITE_CONTACT_FORM' 
                 AND response_json->>'is_inquiry_for_data_science_certification' = 'btech_in_cs_ai_from_nst' THEN 'CF_NST'
            WHEN response_type = 'PUBLIC_WEBSITE_CHATBOT' THEN 'CB'
            WHEN response_type = 'PUBLIC_WEBSITE_CHATBOT' THEN 'CB'
            WHEN response_type = 'PUBLIC_WEBSITE_DS_HOME_REQUEST_CALLBACK_FORM' 
                 AND response_json->>'from' = 'download_brochure' THEN 'DB'
            ELSE 'UNKNOWN'
        END AS inbound_key
    FROM 
        marketing_genericformresponse
    WHERE
    (
        (
            response_type = 'PUBLIC_WEBSITE_HOME_REQUEST_CALLBACK_FORM' 
            AND response_json->>'course_type_interested_in' = 'ds'
        )
        OR response_type = 'PUBLIC_WEBSITE_DS_HOME_REQUEST_CALLBACK_FORM'
        OR response_type = 'PUBLIC_WEBSITE_CHATBOT'
        OR 
        ( response_type = 'PUBLIC_WEBSITE_CONTACT_FORM'
            AND response_json->>'is_inquiry_for_data_science_certification' = 'professional_certification_in_ds_ai')
    )
),
UserSignIn AS (
    SELECT 
        auth_user.email,
        users_userprofile.phone,
        CONCAT(auth_user.first_name, ' ', auth_user.last_name) AS "Name",
        auth_user.id,
        auth_user.date_joined,
        users_extendeduserprofile.graduation_year AS PGY,
        users_userprofile.utm_param_json->>'course_structure_slug' AS course_slug,
        users_userprofile.utm_param_json->>'marketing_url_structure_slug' AS marketing_slug,
        users_userprofile.utm_param_json->>'utm_source' AS utm_source,
        users_userprofile.utm_param_json->>'utm_medium' AS utm_medium,
        users_userprofile.utm_param_json->>'utm_campaign' AS utm_campaign,
        users_userprofile.utm_param_json->>'utm_referer' AS utm_referer,
        users_userprofile.utm_param_json->>'incoming_course_structure_slug' AS incoming_course_structure_slug,
        users_userprofile.utm_param_json->>'utm_hash' AS utm_hash
    FROM 
        auth_user
    JOIN 
        users_userprofile ON users_userprofile.user_id = auth_user.id
    LEFT JOIN 
        users_extendeduserprofile ON users_extendeduserprofile.user_id = auth_user.id
)
SELECT
    r.id,
    r.response_type,
    r.from_source,
    r.email,
    r.full_name,
    r.phone_number,
    r.current_status,
    r.graduation_year,
    r.highest_qualification,
    r.course_type_interested_in,
    r.is_inquiry_for_data_science_certification,
    r.created_at AS form_created_at,
    u.date_joined AS user_date_joined,
    r.inbound_key,
    CASE 
        WHEN u.date_joined < r.created_at THEN 'Signed In First'
        ELSE 'Filled Form First'
    END AS first_action,
    CASE 
        WHEN graduation_year ~ '^\d+$' AND (CAST(graduation_year AS INT) BETWEEN 2017 AND 2024) AND highest_qualification NOT IN ('12th','diploma') AND highest_qualification IS NOT NULL THEN True
    END AS eligible
FROM
    RankedResponses r
LEFT JOIN
    UserSignIn u ON r.email = u.email
WHERE
    r.rn = 1
    AND (CASE 
            WHEN u.date_joined < r.created_at THEN 'Signed In First'
            ELSE 'Filled Form First'
         END) = 'Filled Form First'
ORDER BY
    form_created_at DESC;
        ''',
    dag=dag
)

extract_python_data = PythonOperator(
    task_id='extract_python_data',
    python_callable=extract_data_to_nested,
    provide_context=True,
    dag=dag
)

drop_table = PostgresOperator(
    task_id='drop_table',
    postgres_conn_id='postgres_result_db',
    sql='''DROP TABLE IF EXISTS ds_inbound_form_filled;
    ''',
    dag=dag
)

drop_table >> create_table >> transform_data >> extract_python_data
