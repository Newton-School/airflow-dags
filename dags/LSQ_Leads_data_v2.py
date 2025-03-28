from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 24),
}


def extract_data_to_nested(**kwargs):

    pg_hook = PostgresHook(postgres_conn_id='postgres_result_db')
    pg_conn = pg_hook.get_conn()
    pg_cursor = pg_conn.cursor()
    ti = kwargs['ti']
    transform_data_output = ti.xcom_pull(task_ids='transform_data')
    for transform_row in transform_data_output:
        pg_cursor.execute(
            'INSERT INTO lsq_leads_x_activities_v2 ('
              'table_unique_key,'
              'prospect_id,'
              'activity_id,'
              'sales_user_email,'
              'prospect_email,'
              'CRM_user_role,'
              'lead_created_on,'
              'event,'
              'modified_on,'
              'prospect_stage,'
              'lead_owner,'
              'lead_owner_id,'
              'lead_sub_status,'
              'lead_last_call_status,'
              'lead_last_call_sub_status,'
              'lead_last_call_connection_status,'
              'reactivation_bucket,'
              'reactivation_date,'
              'source_intended_course,'
              'intended_course,'
              'created_by_name,'
              'event_name,'
              'notable_event_description,'
              'previous_stage,'
              'current_stage,'
              'call_type,'
              'caller,'
              'duration,'
              'call_notes,'
              'previous_owner,'
              'current_owner,'
              'has_attachments,'
              'mx_custom_1,'
              'mx_custom_2,'
              'mx_custom_status,'
              'mx_custom_3,'
              'mx_custom_4,'
              'mx_custom_5,'
              'mx_custom_6,'
              'mx_custom_7,'
              'mx_custom_8,'
              'mx_custom_9,'
              'mx_custom_10,'
              'mx_custom_11,'
              'mx_custom_12,'
              'mx_custom_13,'
              'mx_custom_14,'
              'mx_custom_15,'
              'mx_custom_16,'
              'mx_custom_17,'
              'mx_priority_status,'
              'mx_rfd_date,'
              'mx_identifer,'
              'mx_organic_inbound,'
              'mx_entrance_exam_marks,'
              'mx_lead_quality_grade,'
              'mx_lead_inherent_intent,'
              'mx_test_date_n_time,'
              'mx_lead_type,'
              'mx_utm_source,'
              'mx_utm_medium,'
              'score,'
              'mx_phoenix_identifer,'
              'mx_phoenix_lead_assigned_date,'
              'mx_prospect_status,'
              'mx_reactivation_source,'
              'mx_reactivation_date,'
              'mx_lead_status,'
              'mx_pmm_identifier,'
              'mx_city,'
              'mx_date_of_birth'
            ')'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'
            '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'
            '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (table_unique_key) do update set '
            'sales_user_email = EXCLUDED.sales_user_email,'
            'prospect_email = EXCLUDED.prospect_email,'
            'CRM_user_role = EXCLUDED.CRM_user_role,'
            'lead_created_on = EXCLUDED.lead_created_on,'
            'event = EXCLUDED.event,'
            'modified_on = EXCLUDED.modified_on,'
            'prospect_stage = EXCLUDED.prospect_stage,'
            'lead_owner = EXCLUDED.lead_owner,'
            'lead_owner_id = EXCLUDED.lead_owner_id,'
            'lead_sub_status = EXCLUDED.lead_sub_status,'
            'lead_last_call_status = EXCLUDED.lead_last_call_status,'
            'lead_last_call_sub_status = EXCLUDED.lead_last_call_sub_status,'
            'lead_last_call_connection_status = EXCLUDED.lead_last_call_connection_status,'
            'reactivation_bucket = EXCLUDED.reactivation_bucket,'
            'reactivation_date = EXCLUDED.reactivation_date,'
            'source_intended_course = EXCLUDED.source_intended_course,'
            'intended_course = EXCLUDED.intended_course,'
            'created_by_name = EXCLUDED.created_by_name,'
            'event_name = EXCLUDED.event_name,'
            'notable_event_description = EXCLUDED.notable_event_description,'
            'previous_stage = EXCLUDED.previous_stage,'
            'current_stage = EXCLUDED.current_stage,'
            'call_type = EXCLUDED.call_type,'
            'caller = EXCLUDED.caller,'
            'duration = EXCLUDED.duration,'
            'call_notes = EXCLUDED.call_notes,'
            'previous_owner = EXCLUDED.previous_owner,'
            'current_owner = EXCLUDED.current_owner,'
            'has_attachments = EXCLUDED.has_attachments,'
            'mx_custom_1 = EXCLUDED.mx_custom_1,'
            'mx_custom_2 = EXCLUDED.mx_custom_2,'
            'mx_custom_status = EXCLUDED.mx_custom_status,'
            'mx_custom_3 = EXCLUDED.mx_custom_3,'
            'mx_custom_4 = EXCLUDED.mx_custom_4,'
            'mx_custom_5 = EXCLUDED.mx_custom_5,'
            'mx_custom_6 = EXCLUDED.mx_custom_6,'
            'mx_custom_7 = EXCLUDED.mx_custom_7,'
            'mx_custom_8 = EXCLUDED.mx_custom_8,'
            'mx_custom_9 = EXCLUDED.mx_custom_9,'
            'mx_custom_10 = EXCLUDED.mx_custom_10,'
            'mx_custom_11 = EXCLUDED.mx_custom_11,'
            'mx_custom_12 = EXCLUDED.mx_custom_12,'
            'mx_custom_13 = EXCLUDED.mx_custom_13,'
            'mx_custom_14 = EXCLUDED.mx_custom_14,'
            'mx_custom_15 = EXCLUDED.mx_custom_15,'
            'mx_custom_16 = EXCLUDED.mx_custom_16,'
            'mx_custom_17 = EXCLUDED.mx_custom_17,'
            'mx_priority_status = EXCLUDED.mx_priority_status,'
            'mx_rfd_date = EXCLUDED.mx_rfd_date,'
            'mx_identifer = EXCLUDED.mx_identifer,'
            'mx_organic_inbound = EXCLUDED.mx_organic_inbound,'
            'mx_entrance_exam_marks = EXCLUDED.mx_entrance_exam_marks,'
            'mx_lead_quality_grade = EXCLUDED.mx_lead_quality_grade,'
            'mx_lead_inherent_intent = EXCLUDED.mx_lead_inherent_intent,'
            'mx_test_date_n_time = EXCLUDED.mx_test_date_n_time,'
            'mx_lead_type = EXCLUDED.mx_lead_type,'
            'mx_utm_source = EXCLUDED.mx_utm_source,'
            'mx_utm_medium = EXCLUDED.mx_utm_medium,'
            'score = EXCLUDED.score,'
            'mx_phoenix_identifer = EXCLUDED.mx_phoenix_identifer,'
            'mx_phoenix_lead_assigned_date = EXCLUDED.mx_phoenix_lead_assigned_date,'
            'mx_prospect_status = EXCLUDED.mx_prospect_status,'
            'mx_reactivation_source = EXCLUDED.mx_reactivation_source,'
            'mx_reactivation_date = EXCLUDED.mx_reactivation_date,'
            'mx_lead_status = EXCLUDED.mx_lead_status,'
            'mx_pmm_identifier = EXCLUDED.mx_pmm_identifier,'
            'mx_city = EXCLUDED.mx_city,'
            'mx_date_of_birth = EXCLUDED.mx_date_of_birth;',
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
                transform_row[57],
                transform_row[58],
                transform_row[59],
                transform_row[60],
                transform_row[61],
                transform_row[62],
                transform_row[63],
                transform_row[64],
                transform_row[65],
                transform_row[66],
                transform_row[67],
                transform_row[68],
                transform_row[69],
                transform_row[70]
            )
        )
    pg_conn.commit()


dag = DAG(
    'LSQ_Leads_and_activities_v2',
    default_args=default_args,
    description='An Analytics Data Layer DAG for Leads and their activities. Data Source = Leadsquared',
    schedule_interval='0 22 * * *',
    catchup=False
)

# Task 1: Create table if not exists
create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS lsq_leads_x_activities_v2 (
            id serial,
            table_unique_key varchar(512) not null PRIMARY KEY,
            prospect_id varchar(512),
            activity_id varchar(512),
            sales_user_email varchar(512),
            prospect_email varchar(512),
            CRM_user_role varchar(512),
            lead_created_on TIMESTAMP,
            event varchar(512),
            modified_on TIMESTAMP,
            prospect_stage varchar(512),
            lead_owner varchar(512),
            lead_owner_id varchar(512),
            lead_sub_status varchar(512),
            lead_last_call_status varchar(512),
            lead_last_call_sub_status varchar(512),
            lead_last_call_connection_status varchar(512),
            reactivation_bucket varchar(512),
            reactivation_date TIMESTAMP,
            source_intended_course varchar(512),
            intended_course varchar(512),
            created_by_name varchar(512),
            event_name varchar(512),
            notable_event_description varchar(10000),
            previous_stage varchar(512),
            current_stage varchar(512),
            call_type varchar(512),
            caller varchar(512),
            duration real,
            call_notes varchar(3000),
            previous_owner varchar(512),
            current_owner varchar(512),
            has_attachments boolean,
            mx_custom_1 varchar(512),
            mx_custom_2 varchar(512),
            mx_custom_status varchar(512),
            mx_custom_3 varchar(512),
            mx_custom_4 varchar(512),
            mx_custom_5 varchar(2000),
            mx_custom_6 varchar(512),
            mx_custom_7 varchar(512),
            mx_custom_8 varchar(512),
            mx_custom_9 varchar(10000),
            mx_custom_10 varchar(10000),
            mx_custom_11 varchar(512),
            mx_custom_12 varchar(512),
            mx_custom_13 varchar(512),
            mx_custom_14 varchar(512),
            mx_custom_15 varchar(512),
            mx_custom_16 varchar(512),
            mx_custom_17 varchar(512),
            mx_priority_status varchar(512),
            mx_rfd_date varchar(512),
            mx_identifer varchar(512),
            mx_organic_inbound varchar(512),
            mx_entrance_exam_marks varchar(512),
            mx_lead_quality_grade varchar(512),
            mx_lead_inherent_intent varchar(512),
            mx_test_date_n_time TIMESTAMP,
            mx_lead_type varchar(512),
            mx_utm_source varchar(512),
            mx_utm_medium varchar(512),
            score varchar(512),
            mx_phoenix_identifer varchar(512),
            mx_phoenix_lead_assigned_date TIMESTAMP,
            mx_prospect_status varchar(512),
            mx_reactivation_source  varchar(512),
            mx_reactivation_date TIMESTAMP,
            mx_lead_status varchar(512),
            mx_pmm_identifier varchar(512),
            mx_city varchar(512),
            mx_date_of_birth TIMESTAMP
        );
    ''',
    dag=dag
)

# Task 2: Transform data
transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_lsq_leads',
    sql='''
            select
                concat(l2.prospectid,l.activityid) as table_unique_key,
                l2.prospectid as prospect_id,
                l.activityid as activity_id,
                l2.sales_user_email as sales_user_email,
                l2.prospect_email as prospect_email,
                l2.role as CRM_user_role,
                l2.createdon as lead_created_on,
                eventname as event,
                l.createdon::timestamp + INTERVAL '5 hours 30 minutes' as modified_on,
                l2.prospectstage as prospect_stage,
                l2.owneridname as lead_owner,
                l2.ownerid as lead_owner_id,
                l2.mx_substatus as lead_sub_status,
                l2.mx_last_call_status as lead_last_call_status,
                l2.mx_last_call_sub_status as lead_last_call_sub_status,
                l2.mx_last_call_connection_status as lead_last_call_connection_status,
                mx_reactivation_bucket as reactivation_bucket,
                mx_reactivation_date as reactivation_date,
                mx_source_intended_course as source_intended_course,
                case 
                  when lower(mx_source_intended_course) like ('%fsd%') then 'FSD'
                  when lower(mx_source_intended_course) like ('%full%') then 'FSD'
                  when lower(mx_source_intended_course) like ('%data%') then 'DS'
                  when lower(mx_source_intended_course) like ('%ds%') then 'DS'
                  when lower(mx_source_intended_course) like '%bs%' then 'Bachelors' 
                end as intended_course,
                coalesce(cast((CASE when jsonb_typeof(activitydata) <> 'object' AND EXISTS ( SELECT 1 FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'CreatedBy')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'CreatedBy' LIMIT 1 ) ELSE null end) as varchar),
                      cast((CASE when jsonb_typeof(activitydata) <> 'object' AND EXISTS ( SELECT 1 FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'CreatedByName')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'CreatedByName' LIMIT 1 ) ELSE null end)as varchar)) AS created_by_name,
                        
                (CASE when jsonb_typeof(activitydata) <> 'object' AND EXISTS ( SELECT 1 FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'EventName')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'EventName' LIMIT 1 ) ELSE null end) AS event_name,
                        
                (CASE when jsonb_typeof(activitydata) <> 'object' AND EXISTS ( SELECT 1 FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'NotableEventDescription')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'NotableEventDescription' LIMIT 1 ) ELSE null end) AS notable_event_description,
                        
                (CASE when jsonb_typeof(activitydata) <> 'object' AND EXISTS ( SELECT 1 FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'PreviousStage')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'PreviousStage' LIMIT 1 ) ELSE null end) AS previous_stage,
                        
                (CASE when jsonb_typeof(activitydata) <> 'object' AND EXISTS ( SELECT 1 FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'CurrentStage')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'CurrentStage' LIMIT 1 ) ELSE null end) AS current_stage,
                        
                (CASE when jsonb_typeof(activitydata) <> 'object' AND EXISTS ( SELECT 1 FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'CallType')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'CallType' LIMIT 1 ) ELSE null end) AS call_type,
                        
                (CASE when jsonb_typeof(activitydata) <> 'object' AND EXISTS ( SELECT 1 FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'Caller')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'Caller' LIMIT 1 ) ELSE null end) AS caller,
                        
                (CASE when jsonb_typeof(activitydata) <> 'object' AND EXISTS ( SELECT 1 FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'Duration')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'Duration' LIMIT 1 ) ELSE null end) AS duration,
                        
                (CASE when jsonb_typeof(activitydata) <> 'object' AND EXISTS ( SELECT 1 FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'CallNotes')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'CallNotes' LIMIT 1 ) ELSE null end) AS call_notes,
                        
                (CASE when jsonb_typeof(activitydata) <> 'object' AND EXISTS ( SELECT 1 FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'PreviousOwner')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'PreviousOwner' LIMIT 1 ) ELSE null end) AS previous_owner,
                        
                (CASE when jsonb_typeof(activitydata) <> 'object' AND EXISTS ( SELECT 1 FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'CurrentOwner')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'CurrentOwner' LIMIT 1 ) ELSE null end) AS current_owner,
                        
                (CASE when jsonb_typeof(activitydata) <> 'object' AND EXISTS ( SELECT 1 FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'HasAttachments')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'HasAttachments' LIMIT 1 ) ELSE null end) AS has_attachments,
                        
                (CASE when jsonb_typeof(activitycustomfields) <> 'object' AND EXISTS (SELECT 1
                      FROM jsonb_array_elements(activitycustomfields) AS message WHERE (message->>'Key')::varchar = 'mx_Custom_1')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitycustomfields) AS message
                      WHERE (message->>'Key')::varchar = 'mx_Custom_1' LIMIT 1 )ELSE null END) AS mx_custom_1,
                      
                (CASE WHEN jsonb_typeof(activitycustomfields) <> 'object' AND EXISTS (SELECT 1
                      FROM jsonb_array_elements(activitycustomfields) AS message WHERE (message->>'Key')::varchar = 'mx_Custom_2')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitycustomfields) AS message
                      WHERE (message->>'Key')::varchar = 'mx_Custom_2' LIMIT 1 )ELSE null END) AS mx_custom_2,
                      
                (CASE WHEN jsonb_typeof(activitycustomfields) <> 'object' AND EXISTS (SELECT 1
                      FROM jsonb_array_elements(activitycustomfields) AS message WHERE (message->>'Key')::varchar = 'Status')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitycustomfields) AS message
                      WHERE (message->>'Key')::varchar = 'Status' LIMIT 1 )ELSE null END) AS mx_custom_status,
                      
                (CASE WHEN jsonb_typeof(activitycustomfields) <> 'object' AND EXISTS (SELECT 1
                      FROM jsonb_array_elements(activitycustomfields) AS message WHERE (message->>'Key')::varchar = 'mx_Custom_3')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitycustomfields) AS message
                      WHERE (message->>'Key')::varchar = 'mx_Custom_3' LIMIT 1 )ELSE null END) AS mx_custom_3,
                      
                (CASE WHEN jsonb_typeof(activitycustomfields) <> 'object' AND EXISTS (SELECT 1
                      FROM jsonb_array_elements(activitycustomfields) AS message WHERE (message->>'Key')::varchar = 'mx_Custom_4')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitycustomfields) AS message
                      WHERE (message->>'Key')::varchar = 'mx_Custom_4' LIMIT 1 )ELSE null END) AS mx_custom_4,
                      
                (CASE WHEN jsonb_typeof(activitycustomfields) <> 'object' AND EXISTS (SELECT 1
                      FROM jsonb_array_elements(activitycustomfields) AS message WHERE (message->>'Key')::varchar = 'mx_Custom_5')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitycustomfields) AS message
                      WHERE (message->>'Key')::varchar = 'mx_Custom_5' LIMIT 1 )ELSE null END) AS mx_custom_5,
                      
                (CASE WHEN jsonb_typeof(activitycustomfields) <> 'object' AND EXISTS (SELECT 1
                      FROM jsonb_array_elements(activitycustomfields) AS message WHERE (message->>'Key')::varchar = 'mx_Custom_6')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitycustomfields) AS message
                      WHERE (message->>'Key')::varchar = 'mx_Custom_6' LIMIT 1 )ELSE null END) AS mx_custom_6,
                      
                (CASE WHEN jsonb_typeof(activitycustomfields) <> 'object' AND EXISTS (SELECT 1
                      FROM jsonb_array_elements(activitycustomfields) AS message WHERE (message->>'Key')::varchar = 'mx_Custom_7')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitycustomfields) AS message
                      WHERE (message->>'Key')::varchar = 'mx_Custom_7' LIMIT 1 )ELSE null END) AS mx_custom_7,
            
                (CASE WHEN jsonb_typeof(activitycustomfields) <> 'object' AND EXISTS (SELECT 1
                      FROM jsonb_array_elements(activitycustomfields) AS message WHERE (message->>'Key')::varchar = 'mx_Custom_8')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitycustomfields) AS message
                      WHERE (message->>'Key')::varchar = 'mx_Custom_8' LIMIT 1 )ELSE null END) AS mx_custom_8,
            
                (CASE WHEN jsonb_typeof(activitycustomfields) <> 'object' AND EXISTS (SELECT 1
                      FROM jsonb_array_elements(activitycustomfields) AS message WHERE (message->>'Key')::varchar = 'mx_Custom_9')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitycustomfields) AS message
                      WHERE (message->>'Key')::varchar = 'mx_Custom_9' LIMIT 1 )ELSE null END) AS mx_custom_9,
                      
                (CASE WHEN jsonb_typeof(activitycustomfields) <> 'object' AND EXISTS (SELECT 1
                      FROM jsonb_array_elements(activitycustomfields) AS message WHERE (message->>'Key')::varchar = 'mx_Custom_10')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitycustomfields) AS message
                      WHERE (message->>'Key')::varchar = 'mx_Custom_10' LIMIT 1 )ELSE null END) AS mx_custom_10,
                (CASE WHEN jsonb_typeof(activitycustomfields) <> 'object' AND EXISTS (SELECT 1
                      FROM jsonb_array_elements(activitycustomfields) AS message WHERE (message->>'Key')::varchar = 'mx_Custom_11')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitycustomfields) AS message
                      WHERE (message->>'Key')::varchar = 'mx_Custom_11' LIMIT 1 )ELSE null END) AS mx_custom_11,
                      
                (CASE WHEN jsonb_typeof(activitycustomfields) <> 'object' AND EXISTS (SELECT 1
                      FROM jsonb_array_elements(activitycustomfields) AS message WHERE (message->>'Key')::varchar = 'mx_Custom_12')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitycustomfields) AS message
                      WHERE (message->>'Key')::varchar = 'mx_Custom_12' LIMIT 1 )ELSE null END) AS mx_custom_12,
                      
                (CASE WHEN jsonb_typeof(activitycustomfields) <> 'object' AND EXISTS (SELECT 1
                      FROM jsonb_array_elements(activitycustomfields) AS message WHERE (message->>'Key')::varchar = 'mx_Custom_13')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitycustomfields) AS message
                      WHERE (message->>'Key')::varchar = 'mx_Custom_13' LIMIT 1 )ELSE null END) AS mx_custom_13,
                      
                (CASE WHEN jsonb_typeof(activitycustomfields) <> 'object' AND EXISTS (SELECT 1
                      FROM jsonb_array_elements(activitycustomfields) AS message WHERE (message->>'Key')::varchar = 'mx_Custom_14')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitycustomfields) AS message
                      WHERE (message->>'Key')::varchar = 'mx_Custom_14' LIMIT 1 )ELSE null END) AS mx_custom_14,
                      
                (CASE WHEN jsonb_typeof(activitycustomfields) <> 'object' AND EXISTS (SELECT 1
                      FROM jsonb_array_elements(activitycustomfields) AS message WHERE (message->>'Key')::varchar = 'mx_Custom_15')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitycustomfields) AS message
                      WHERE (message->>'Key')::varchar = 'mx_Custom_15' LIMIT 1 )ELSE null END) AS mx_custom_15,
                      
                (CASE WHEN jsonb_typeof(activitycustomfields) <> 'object' AND EXISTS (SELECT 1
                      FROM jsonb_array_elements(activitycustomfields) AS message WHERE (message->>'Key')::varchar = 'mx_Custom_16')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitycustomfields) AS message
                      WHERE (message->>'Key')::varchar = 'mx_Custom_16' LIMIT 1 )ELSE null END) AS mx_custom_16,
                      
                (CASE WHEN jsonb_typeof(activitycustomfields) <> 'object' AND EXISTS (SELECT 1
                      FROM jsonb_array_elements(activitycustomfields) AS message WHERE (message->>'Key')::varchar = 'mx_Custom_17')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitycustomfields) AS message
                      WHERE (message->>'Key')::varchar = 'mx_Custom_17' LIMIT 1 )ELSE null END) AS mx_custom_17,
                    
                l2.mx_priority_status,
                l2.mx_rfd_date, 
                l2.mx_identifer,
                l2.mx_organic_inbound,
                l2.mx_entrance_exam_marks,
                l2.mx_lead_quality_grade,
                l2.mx_lead_inherent_intent,
                l2.mx_test_date_n_time,
                l2.mx_lead_type,
                l2.mx_utm_source,
                l2.mx_utm_medium,
                l2.score,
                l2.mx_phoenix_identifer,
                l2.mx_phoenix_lead_assigned_date,
                l2.mx_prospect_status,
                l2.mx_reactivation_source,
                l2.mx_reactivation_date,
                l2.mx_lead_status,
                l2.mx_pmm_identifier,
                l2.mx_city,
                l2.mx_date_of_birth
                    
            FROM (
                select *
                from (
                SELECT 
                     *,
                    TO_CHAR(createdon::timestamp + INTERVAL '5 hours 30 minutes', 'YYYY-MM-DD HH24:MI:SS') AS createdon_ist
                FROM leadsquareactivity 
            ) sub
            WHERE
                TO_TIMESTAMP(sub.createdon_ist, 'YYYY-MM-DD HH24:MI:SS') >= '2024-10-01 00:00:00'
                AND TO_TIMESTAMP(sub.createdon_ist, 'YYYY-MM-DD HH24:MI:SS') < '2025-03-28 00:00:00'
            ) as l
            left join (
                select * from (
                    select distinct
                        ld.prospectid,
                        lu.emailaddress as sales_user_email,
                        ld.emailaddress as prospect_email,
                        ld.createdon::timestamp + INTERVAL '5 hours 30 minutes' as createdon,
                        ld.prospectstage,
                        lu.role,
                        ld.ownerid,
                        ld.owneridname,
                        ld.mx_substatus,
                        ld.mx_last_call_status,
                        ld.mx_last_call_sub_status,
                        ld.mx_last_call_connection_status,
                        ld.mx_priority_status,
                        ld.mx_rfd_date, 
                        ld.mx_identifer,
                        ld.mx_organic_inbound,
                        ld.mx_entrance_exam_marks,
                        ld.mx_lead_quality_grade,
                        ld.mx_lead_inherent_intent,
                        ld.mx_test_date_n_time,
                        ld.mx_lead_type,
                        ld.mx_reactivation_bucket,
                        ld.mx_source_intended_course,
                        ld.mx_utm_source,
                        ld.mx_utm_medium,
                        ld.mx_utm_campaign,
                        ld.score,
                        ld.mx_phoenix_identifer,
                        ld.mx_phoenix_lead_assigned_date,
                        ld.mx_prospect_status,
                        ld.mx_reactivation_source,
                        ld.mx_reactivation_date,
                        ld.mx_lead_status,
                        ld.mx_pmm_identifier,
                        ld.mx_city,
                        ld.mx_date_of_birth,
                        ld.modifiedon,
                        row_number() over (partition by ld.prospectid order by ld.modifiedon desc) as rn
                        from leadsquareleadsdata ld left join leadsquareusers lu
                        on lu.Userid = ld.ownerid 
                ) a
                where rn = 1
            ) as l2 
             on l.relatedprospectid = l2.prospectid 
            order by 2,5;
        ''',
    dag=dag
)

# Task 4: Extract data to nested
extract_python_data = PythonOperator(
    task_id='extract_python_data',
    python_callable=extract_data_to_nested,
    provide_context=True,
    dag=dag
)

# Define Task Dependencies
create_table >> transform_data >> extract_python_data

