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
            'INSERT INTO lsq_leads_x_activities ('
              'table_unique_key,'
              'prospect_id,'
              'activity_id,'
              'email_address,'
              'lead_created_on,'
              'event,'
              'modified_on,'
              'prospect_stage,'
              'lead_owner,'
              'lead_sub_status,'
              'lead_last_call_status,'
              'lead_last_call_sub_status,'
              'lead_last_call_connection_status,'
              'mid_funnel_count,'
              'mid_funnel_buckets,'
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
              'mx_total_fees,'
              'mx_total_revenue,'
              'mx_doc_approved,'
              'mx_doc_collected,'
              'mx_cibil_check,'
              'mx_bucket,'
              'mx_icp,'
              'mx_identifer,'
              'mx_organic_inbound,'
              'mx_entrance_exam_marks,'
              'mx_lead_quality_grade,'
              'mx_lead_inherent_intent,'
              'mx_test_date_n_time,'
              'mx_lead_type'
            ')'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'
            '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (table_unique_key) do update set '
            'email_address = EXCLUDED.email_address,'
            'lead_created_on = EXCLUDED.lead_created_on,'
            'event = EXCLUDED.event,'
            'modified_on = EXCLUDED.modified_on,'
            'prospect_stage = EXCLUDED.prospect_stage,'
            'lead_owner = EXCLUDED.lead_owner,'
            'lead_sub_status = EXCLUDED.lead_sub_status,'
            'lead_last_call_status = EXCLUDED.lead_last_call_status,'
            'lead_last_call_sub_status = EXCLUDED.lead_last_call_sub_status,'
            'lead_last_call_connection_status = EXCLUDED.lead_last_call_connection_status,'
            'mid_funnel_count = EXCLUDED.mid_funnel_count,'
            'mid_funnel_buckets = EXCLUDED.mid_funnel_buckets,'
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
            'mx_total_fees = EXCLUDED.mx_total_fees,'
            'mx_total_revenue = EXCLUDED.mx_total_revenue,'
            'mx_doc_approved = EXCLUDED.mx_doc_approved,'
            'mx_doc_collected = EXCLUDED.mx_doc_collected,'
            'mx_cibil_check = EXCLUDED.mx_cibil_check,'
            'mx_bucket = EXCLUDED.mx_bucket,'
            'mx_icp = EXCLUDED.mx_icp,'
            'mx_identifer = EXCLUDED.mx_identifer,'
            'mx_organic_inbound = EXCLUDED.mx_organic_inbound,'
            'mx_entrance_exam_marks = EXCLUDED.mx_entrance_exam_marks,'
            'mx_lead_quality_grade = EXCLUDED.mx_lead_quality_grade,'
            'mx_lead_inherent_intent = EXCLUDED.mx_lead_inherent_intent,'
            'mx_test_date_n_time = EXCLUDED.mx_test_date_n_time,'
            'mx_lead_type = EXCLUDED.mx_lead_type;',

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
                transform_row[64]
            )
        )
    pg_conn.commit()


dag = DAG(
    'LSQ_Leads_and_activities',
    default_args=default_args,
    description='An Analytics Data Layer DAG for Leads and their activities. Data Source = Leadsquared',
    schedule_interval='0 */2 * * *',
    catchup=False
)

# Task 1: Delete existing records for the current date
delete_table = PostgresOperator(
    task_id='delete_table',
    postgres_conn_id='postgres_result_db',
    sql="delete from lsq_leads_x_activities where modified_on::date >= current_date - interval '1' day;",
    dag=dag
)

# Task 2: Create table if not exists
create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS lsq_leads_x_activities (
            id serial,
            table_unique_key varchar(512) not null PRIMARY KEY,
            prospect_id varchar(512),
            activity_id varchar(512),
            email_address varchar(512),
            lead_created_on TIMESTAMP,
            event varchar(512),
            modified_on TIMESTAMP,
            prospect_stage varchar(512),
            lead_owner varchar(512),
            lead_sub_status varchar(512),
            lead_last_call_status varchar(512),
            lead_last_call_sub_status varchar(512),
            lead_last_call_connection_status varchar(512),
            mid_funnel_count int,
            mid_funnel_buckets varchar(512),
            reactivation_bucket varchar(512),
            reactivation_date TIMESTAMP,
            source_intended_course varchar(512),
            intended_course varchar(512),
            created_by_name varchar(512),
            event_name varchar(512),
            notable_event_description varchar(5000),
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
            mx_custom_9 varchar(5000),
            mx_custom_10 varchar(5000),
            mx_custom_11 varchar(512),
            mx_custom_12 varchar(512),
            mx_custom_13 varchar(512),
            mx_custom_14 varchar(512),
            mx_custom_15 varchar(512),
            mx_custom_16 varchar(512),
            mx_custom_17 varchar(512),
            mx_priority_status varchar(512),
            mx_rfd_date varchar(512),
            mx_total_fees varchar(512),
            mx_total_revenue varchar(512),
            mx_doc_approved varchar(512),
            mx_doc_collected varchar(512),
            mx_cibil_check varchar(512),
            mx_bucket varchar(512),
            mx_icp varchar(512),
            mx_identifer varchar(512),
            mx_organic_inbound varchar(512),
            mx_entrance_exam_marks varchar(512),
            mx_lead_quality_grade varchar(512),
            mx_lead_inherent_intent varchar(512),
            mx_test_date_n_time TIMESTAMP,
            mx_lead_type varchar(512)
        );
    ''',
    dag=dag
)



# Task 3: Alter table to increase column sizes
alter_table = PostgresOperator(
    task_id='alter_table',
    postgres_conn_id='postgres_result_db',
    sql='''
        ALTER TABLE lsq_leads_x_activities 
        ALTER COLUMN notable_event_description TYPE varchar(10000),
        ALTER COLUMN mx_custom_9 TYPE varchar(10000),
        ALTER COLUMN mx_custom_10 TYPE varchar(10000);
    ''',
    dag=dag
)

# Task 4: Transform data
transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_lsq_leads',
    sql='''
        select
            distinct 
            concat(l2.prospectid,l.activityid) as table_unique_key,
            l2.prospectid as prospect_id,
            l.activityid as activity_id,
            l2.emailaddress as email_address,
            l2.createdon as lead_created_on,
            eventname as event,
            l.createdon::timestamp + INTERVAL '5 hours 30 minutes' as modified_on,
            l2.prospectstage as prospect_stage,
            l2.owneridname as lead_owner,
            l2.mx_substatus as lead_sub_status,
            l2.mx_last_call_status as lead_last_call_status,
            l2.mx_last_call_sub_status as lead_last_call_sub_status,
            l2.mx_last_call_connection_status as lead_last_call_connection_status,
            mx_mid_funnel_count as mid_funnel_count,
            mx_mid_funnel_buckets as mid_funnel_buckets,
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
            l2.mx_total_fees,
            l2.mx_total_revenue,
            l2.mx_doc_approved,
            l2.mx_doc_collected,
            l2.mx_cibil_check,
            l2.mx_bucket,
            l2.mx_icp,
            l2.mx_identifer,
            l2.mx_organic_inbound,
            l2.mx_entrance_exam_marks,
            l2.mx_lead_quality_grade,
            l2.mx_lead_inherent_intent,
            l2.mx_test_date_n_time,
            l2.mx_lead_type
                
        FROM (
            select *
            from leadsquareactivity l
            where 
                to_timestamp(l.createdon, 'YYYY-MM-DD hh24:mi:ss') >= current_date - interval '1' day
        ) as l
        left join (
            select * from (
                select distinct
                    prospectid,
                    emailaddress,
                    createdon::timestamp + INTERVAL '5 hours 30 minutes' as createdon,
                    prospectstage,
                    owneridname,
                    mx_substatus,
                    mx_last_call_status,
                    mx_last_call_sub_status,
                    mx_last_call_connection_status,
                    mx_priority_status,
                    mx_rfd_date, 
                    mx_total_fees,
                    mx_total_revenue,
                    mx_doc_approved,
                    mx_doc_collected,
                    mx_cibil_check,
                    mx_bucket,
                    mx_icp,
                    mx_identifer,
                    mx_organic_inbound,
                    mx_entrance_exam_marks,
                    mx_lead_quality_grade,
                    mx_lead_inherent_intent,
                    mx_test_date_n_time,
                    mx_lead_type,
                    mx_mid_funnel_count,
                    mx_mid_funnel_buckets,
                    mx_reactivation_bucket,
                    mx_reactivation_date,
                    mx_source_intended_course,
                    modifiedon,
                    row_number() over (partition by prospectid order by modifiedon desc) as rn
                from leadsquareleadsdata 
            ) a
            where rn = 1
        ) as l2 
            on l.relatedprospectid = l2.prospectid
        order by 2,5;
    ''',
    dag=dag
)

# Task 5: Extract data to nested
extract_python_data = PythonOperator(
    task_id='extract_python_data',
    python_callable=extract_data_to_nested,
    provide_context=True,
    dag=dag
)

# Define Task Dependencies
delete_table >> create_table >> alter_table >> transform_data >> extract_python_data