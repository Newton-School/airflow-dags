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
            'INSERT INTO lsq_leads_x_activities (prospect_id,activity_id,email_address,lead_created_on,'
            'event,modified_on,prospect_stage,mid_funnel_count,mid_funnel_buckets,'
            'reactivation_bucket,reactivation_date,source_intended_course,created_by_name,event_name,'
            'notable_event_description,previous_stage,current_stage,call_type,caller,duration,call_notes,'
            'previous_owner,current_owner,has_attachments,call_status,call_sub_status,call_connection_status)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (activity_id) do nothing;',
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
            )
        )
    pg_conn.commit()


dag = DAG(
    'LSQ_Leads_and_activities',
    default_args=default_args,
    description='An Analytics Data Layer DAG for Leads and their activities. Data Source = Leadsquared',
    schedule_interval='0 10 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS lsq_leads_x_activities (
            id serial,
            prospect_id varchar(256),
            activity_id varchar(256) not null PRIMARY KEY,
            email_address varchar(256),
            lead_created_on TIMESTAMP,
            event varchar(256),
            modified_on TIMESTAMP,
            prospect_stage varchar(256),
            mid_funnel_count int,
            mid_funnel_buckets varchar(256),
            reactivation_bucket varchar(256),
            reactivation_date TIMESTAMP,
            source_intended_course varchar(256),
            created_by_name varchar(256),
            event_name varchar(256),
            notable_event_description varchar(3000),
            previous_stage varchar(256),
            current_stage varchar(256),
            call_type varchar(256),
            caller varchar(256),
            duration real,
            call_notes varchar(3000),
            previous_owner varchar(256),
            current_owner varchar(256),
            has_attachments boolean,
            call_status varchar(256),
            call_sub_status varchar(256),
            call_connection_status varchar(256)
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_lsq_leads',
    sql='''select
            distinct l2.prospectid as prospect_id,
            l.activityid as activity_id,
            l2.emailaddress as email_address,
            l2.createdon as lead_created_on,
            eventname as event,
            l.createdon as modified_on,
            l2.prospectstage as prospect_stage,
            mx_mid_funnel_count as mid_funnel_count,
            mx_mid_funnel_buckets as mid_funnel_buckets,
            mx_reactivation_bucket as reactivation_bucket,
            mx_reactivation_date as reactivation_date,
            mx_source_intended_course as source_intended_course, 
                coalesce(cast((CASE when jsonb_typeof(activitycustomfields) <> 'object' AND EXISTS ( SELECT 1 FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'CreatedBy')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'CreatedBy' LIMIT 1 ) ELSE null end) as varchar),
                      cast((CASE when jsonb_typeof(activitycustomfields) <> 'object' AND EXISTS ( SELECT 1 FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'CreatedByName')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'CreatedByName' LIMIT 1 ) ELSE null end)as varchar)) AS created_by_name,
                        
                (CASE when jsonb_typeof(activitycustomfields) <> 'object' AND EXISTS ( SELECT 1 FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'EventName')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'EventName' LIMIT 1 ) ELSE null end) AS event_name,
                        
                (CASE when jsonb_typeof(activitycustomfields) <> 'object' AND EXISTS ( SELECT 1 FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'NotableEventDescription')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'NotableEventDescription' LIMIT 1 ) ELSE null end) AS notable_event_description,
                        
                (CASE when jsonb_typeof(activitycustomfields) <> 'object' AND EXISTS ( SELECT 1 FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'PreviousStage')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'PreviousStage' LIMIT 1 ) ELSE null end) AS previous_stage,
                        
                (CASE when jsonb_typeof(activitycustomfields) <> 'object' AND EXISTS ( SELECT 1 FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'CurrentStage')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'CurrentStage' LIMIT 1 ) ELSE null end) AS current_stage,
                        
                (CASE when jsonb_typeof(activitycustomfields) <> 'object' AND EXISTS ( SELECT 1 FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'CallType')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'CallType' LIMIT 1 ) ELSE null end) AS call_type,
                        
                (CASE when jsonb_typeof(activitycustomfields) <> 'object' AND EXISTS ( SELECT 1 FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'Caller')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'Caller' LIMIT 1 ) ELSE null end) AS caller,
                        
                (CASE when jsonb_typeof(activitycustomfields) <> 'object' AND EXISTS ( SELECT 1 FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'Duration')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'Duration' LIMIT 1 ) ELSE null end) AS duration,
                        
                (CASE when jsonb_typeof(activitycustomfields) <> 'object' AND EXISTS ( SELECT 1 FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'CallNotes')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'CallNotes' LIMIT 1 ) ELSE null end) AS call_notes,
                        
                (CASE when jsonb_typeof(activitycustomfields) <> 'object' AND EXISTS ( SELECT 1 FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'PreviousOwner')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'PreviousOwner' LIMIT 1 ) ELSE null end) AS previous_owner,
                        
                (CASE when jsonb_typeof(activitycustomfields) <> 'object' AND EXISTS ( SELECT 1 FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'CurrentOwner')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'CurrentOwner' LIMIT 1 ) ELSE null end) AS current_owner,
                        
                (CASE when jsonb_typeof(activitycustomfields) <> 'object' AND EXISTS ( SELECT 1 FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'HasAttachments')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitydata) AS message
                        WHERE (message->>'Key')::varchar = 'HasAttachments' LIMIT 1 ) ELSE null end) AS has_attachments,
                        
                (CASE when jsonb_typeof(activitycustomfields) <> 'object' AND EXISTS (SELECT 1
                      FROM jsonb_array_elements(activitycustomfields) AS message WHERE (message->>'Key')::varchar = 'mx_Custom_1')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitycustomfields) AS message
                      WHERE (message->>'Key')::varchar = 'mx_Custom_1' LIMIT 1 )ELSE null END) AS call_status,
                      
                (CASE WHEN jsonb_typeof(activitycustomfields) <> 'object' AND EXISTS (SELECT 1
                      FROM jsonb_array_elements(activitycustomfields) AS message WHERE (message->>'Key')::varchar = 'mx_Custom_2')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitycustomfields) AS message
                      WHERE (message->>'Key')::varchar = 'mx_Custom_2' LIMIT 1 )ELSE null END) AS call_sub_status,
                      
                (CASE WHEN jsonb_typeof(activitycustomfields) <> 'object' AND EXISTS (SELECT 1
                      FROM jsonb_array_elements(activitycustomfields) AS message WHERE (message->>'Key')::varchar = 'Status')
                    THEN ( SELECT message->>'Value' FROM jsonb_array_elements(activitycustomfields) AS message
                      WHERE (message->>'Key')::varchar = 'Status' LIMIT 1 )ELSE null END) AS call_connection_status
                
            FROM leadsquareactivity l 
            left join leadsquareleadsdata l2 on l2.prospectid = l.relatedprospectid 
            order by l2.prospectid,l.createdon;
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