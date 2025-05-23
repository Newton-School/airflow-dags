from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

# Constants
POSTGRES_CONN_ID = 'postgres_result_db'
POSTGRES_LSQ_CONN_ID = 'postgres_lsq_leads'
TABLE_NAME = 'lsq_leads_x_activities_v2'

# Column definitions for better maintainability
TABLE_COLUMNS = [
        'table_unique_key', 'prospect_id', 'activity_id', 'sales_user_email', 'prospect_email',
        'CRM_user_role', 'lead_created_on', 'event', 'modified_on', 'prospect_stage',
        'lead_owner', 'lead_owner_id', 'lead_sub_status', 'lead_last_call_status',
        'lead_last_call_sub_status', 'lead_last_call_connection_status', 'reactivation_bucket',
        'reactivation_date', 'source_intended_course', 'intended_course', 'created_by_name',
        'event_name', 'notable_event_description', 'previous_stage', 'current_stage',
        'call_type', 'caller', 'duration', 'call_notes', 'previous_owner', 'current_owner',
        'has_attachments', 'mx_custom_1', 'mx_custom_2', 'mx_custom_status', 'mx_custom_3',
        'mx_custom_4', 'mx_custom_5', 'mx_custom_6', 'mx_custom_7', 'mx_custom_8',
        'mx_custom_9', 'mx_custom_10', 'mx_custom_11', 'mx_custom_12', 'mx_custom_13',
        'mx_custom_14', 'mx_custom_15', 'mx_custom_16', 'mx_custom_17', 'mx_priority_status',
        'mx_rfd_date', 'mx_identifer', 'mx_organic_inbound', 'mx_entrance_exam_marks',
        'mx_lead_quality_grade', 'mx_lead_inherent_intent', 'mx_test_date_n_time',
        'mx_lead_type', 'mx_utm_source', 'mx_utm_medium', 'score', 'mx_phoenix_identifer',
        'mx_phoenix_lead_assigned_date', 'mx_prospect_status', 'mx_reactivation_source',
        'mx_reactivation_date', 'mx_lead_status', 'mx_pmm_identifier', 'mx_city',
        'mx_date_of_birth'
]

default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2025, 3, 24),
}


def build_insert_query():
    """Build the INSERT query with proper formatting"""
    columns_str = ','.join(TABLE_COLUMNS)
    placeholders = ','.join(['%s'] * len(TABLE_COLUMNS))

    # Build UPDATE clause for conflict resolution
    update_clauses = [f'{col} = EXCLUDED.{col}' for col in TABLE_COLUMNS[1:]]  # Skip primary key
    update_str = ','.join(update_clauses)

    query = f"""
        INSERT INTO {TABLE_NAME} ({columns_str})
        VALUES ({placeholders})
        ON CONFLICT (table_unique_key) DO UPDATE SET {update_str};
    """
    return query


def extract_data_to_nested(**kwargs):
    """Extract transformed data and load into target table"""
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    with pg_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            ti = kwargs['ti']
            transform_data_output = ti.xcom_pull(task_ids='transform_data')

            if not transform_data_output:
                raise ValueError("No data received from transform_data task")

            insert_query = build_insert_query()

            # Batch insert for better performance
            batch_size = 1000
            for i in range(0, len(transform_data_output), batch_size):
                batch = transform_data_output[i:i + batch_size]
                cursor.executemany(insert_query, batch)

            conn.commit()


# SQL queries as separate strings for better readability
CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    id SERIAL,
    table_unique_key VARCHAR(512) NOT NULL PRIMARY KEY,
    prospect_id VARCHAR(512),
    activity_id VARCHAR(512),
    sales_user_email VARCHAR(512),
    prospect_email VARCHAR(512),
    CRM_user_role VARCHAR(512),
    lead_created_on TIMESTAMP,
    event VARCHAR(512),
    modified_on TIMESTAMP,
    prospect_stage VARCHAR(512),
    lead_owner VARCHAR(512),
    lead_owner_id VARCHAR(512),
    lead_sub_status VARCHAR(512),
    lead_last_call_status VARCHAR(512),
    lead_last_call_sub_status VARCHAR(512),
    lead_last_call_connection_status VARCHAR(512),
    reactivation_bucket VARCHAR(512),
    reactivation_date TIMESTAMP,
    source_intended_course VARCHAR(512),
    intended_course VARCHAR(512),
    created_by_name VARCHAR(512),
    event_name VARCHAR(512),
    notable_event_description VARCHAR(10000),
    previous_stage VARCHAR(512),
    current_stage VARCHAR(512),
    call_type VARCHAR(512),
    caller VARCHAR(512),
    duration REAL,
    call_notes VARCHAR(3000),
    previous_owner VARCHAR(512),
    current_owner VARCHAR(512),
    has_attachments BOOLEAN,
    mx_custom_1 VARCHAR(512),
    mx_custom_2 VARCHAR(512),
    mx_custom_status VARCHAR(512),
    mx_custom_3 VARCHAR(512),
    mx_custom_4 VARCHAR(512),
    mx_custom_5 VARCHAR(2000),
    mx_custom_6 VARCHAR(512),
    mx_custom_7 VARCHAR(512),
    mx_custom_8 VARCHAR(512),
    mx_custom_9 VARCHAR(10000),
    mx_custom_10 VARCHAR(10000),
    mx_custom_11 VARCHAR(512),
    mx_custom_12 VARCHAR(512),
    mx_custom_13 VARCHAR(512),
    mx_custom_14 VARCHAR(512),
    mx_custom_15 VARCHAR(512),
    mx_custom_16 VARCHAR(512),
    mx_custom_17 VARCHAR(512),
    mx_priority_status VARCHAR(512),
    mx_rfd_date VARCHAR(512),
    mx_identifer VARCHAR(512),
    mx_organic_inbound VARCHAR(512),
    mx_entrance_exam_marks VARCHAR(512),
    mx_lead_quality_grade VARCHAR(512),
    mx_lead_inherent_intent VARCHAR(512),
    mx_test_date_n_time TIMESTAMP,
    mx_lead_type VARCHAR(512),
    mx_utm_source VARCHAR(512),
    mx_utm_medium VARCHAR(512),
    score VARCHAR(512),
    mx_phoenix_identifer VARCHAR(512),
    mx_phoenix_lead_assigned_date TIMESTAMP,
    mx_prospect_status VARCHAR(512),
    mx_reactivation_source VARCHAR(512),
    mx_reactivation_date TIMESTAMP,
    mx_lead_status VARCHAR(512),
    mx_pmm_identifier VARCHAR(512),
    mx_city VARCHAR(512),
    mx_date_of_birth TIMESTAMP
);
"""


def get_transform_sql():
    """Generate transform SQL with parameterized date filter"""
    return """
    WITH recent_activities AS (
        SELECT *
        FROM (
            SELECT 
                *,
                TO_CHAR(createdon::timestamp + INTERVAL '5 hours 30 minutes', 'YYYY-MM-DD HH24:MI:SS') AS createdon_ist
            FROM leadsquareactivity 
        ) sub
        WHERE TO_TIMESTAMP(sub.createdon_ist, 'YYYY-MM-DD HH24:MI:SS') >= 
            CASE 
                WHEN '{{ params.start_timestamp }}' != '' THEN '{{ params.start_timestamp }}'::timestamp
                ELSE '{{ data_interval_start }}'::timestamp
            END
          AND TO_TIMESTAMP(sub.createdon_ist, 'YYYY-MM-DD HH24:MI:SS') < 
            CASE 
                WHEN '{{ params.end_timestamp }}' != '' THEN '{{ params.end_timestamp }}'::timestamp
                ELSE '{{ data_interval_end }}'::timestamp
            END
    ),

    lead_assigned_owners AS (
        SELECT 
            relatedprospectid,
            createdon::timestamp + INTERVAL '5 hours 30 minutes' as assigned_modified_on,
            (CASE 
                WHEN jsonb_typeof(activitydata) <> 'object' 
                AND EXISTS (
                    SELECT 1 FROM jsonb_array_elements(activitydata) AS message
                    WHERE (message->>'Key')::varchar = 'CurrentOwner'
                )
                THEN (
                    SELECT message->>'Value' 
                    FROM jsonb_array_elements(activitydata) AS message
                    WHERE (message->>'Key')::varchar = 'CurrentOwner' 
                    LIMIT 1
                ) 
                ELSE null 
            END) AS assigned_current_owner
        FROM recent_activities
        WHERE eventname = 'LeadAssigned'
          AND (CASE 
                WHEN jsonb_typeof(activitydata) <> 'object' 
                AND EXISTS (
                    SELECT 1 FROM jsonb_array_elements(activitydata) AS message
                    WHERE (message->>'Key')::varchar = 'CurrentOwner'
                )
                THEN (
                    SELECT message->>'Value' 
                    FROM jsonb_array_elements(activitydata) AS message
                    WHERE (message->>'Key')::varchar = 'CurrentOwner' 
                    LIMIT 1
                ) 
                ELSE null 
            END) IS NOT NULL
    ),

    latest_lead_assigned AS (
        SELECT DISTINCT
            la1.relatedprospectid,
            ra.createdon::timestamp + INTERVAL '5 hours 30 minutes' as activity_modified_on,
            FIRST_VALUE(la1.assigned_current_owner) OVER (
                PARTITION BY la1.relatedprospectid, ra.activityid 
                ORDER BY la1.assigned_modified_on DESC 
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as fallback_current_owner
        FROM recent_activities ra
        JOIN lead_assigned_owners la1 
            ON la1.relatedprospectid = ra.relatedprospectid 
            AND la1.assigned_modified_on <= ra.createdon::timestamp + INTERVAL '5 hours 30 minutes'
    ),

    leads_data AS (
        SELECT * 
        FROM (
            SELECT DISTINCT
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
                ROW_NUMBER() OVER (PARTITION BY ld.prospectid ORDER BY ld.modifiedon DESC) as rn
            FROM leadsquareleadsdata ld 
            LEFT JOIN leadsquareusers lu ON lu.Userid = ld.ownerid 
        ) a
        WHERE rn = 1
    )

    SELECT
        concat(l2.prospectid, l.activityid) as table_unique_key,
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
        CASE 
            WHEN lower(mx_source_intended_course) LIKE '%fsd%' THEN 'FSD'
            WHEN lower(mx_source_intended_course) LIKE '%full%' THEN 'FSD'
            WHEN lower(mx_source_intended_course) LIKE '%data%' THEN 'DS'
            WHEN lower(mx_source_intended_course) LIKE '%ds%' THEN 'DS'
            WHEN lower(mx_source_intended_course) LIKE '%bs%' THEN 'Bachelors' 
        END as intended_course,

        -- Extract created_by_name
        COALESCE(
            CAST((CASE 
                WHEN jsonb_typeof(activitydata) <> 'object' 
                AND EXISTS (
                    SELECT 1 FROM jsonb_array_elements(activitydata) AS message
                    WHERE (message->>'Key')::varchar = 'CreatedBy'
                )
                THEN (
                    SELECT message->>'Value' 
                    FROM jsonb_array_elements(activitydata) AS message
                    WHERE (message->>'Key')::varchar = 'CreatedBy' 
                    LIMIT 1
                ) 
                ELSE null 
            END) as varchar),
            CAST((CASE 
                WHEN jsonb_typeof(activitydata) <> 'object' 
                AND EXISTS (
                    SELECT 1 FROM jsonb_array_elements(activitydata) AS message
                    WHERE (message->>'Key')::varchar = 'CreatedByName'
                )
                THEN (
                    SELECT message->>'Value' 
                    FROM jsonb_array_elements(activitydata) AS message
                    WHERE (message->>'Key')::varchar = 'CreatedByName' 
                    LIMIT 1
                ) 
                ELSE null 
            END) as varchar)
        ) AS created_by_name,

        -- Extract event_name
        (CASE 
            WHEN jsonb_typeof(activitydata) <> 'object' 
            AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(activitydata) AS message
                WHERE (message->>'Key')::varchar = 'EventName'
            )
            THEN (
                SELECT message->>'Value' 
                FROM jsonb_array_elements(activitydata) AS message
                WHERE (message->>'Key')::varchar = 'EventName' 
                LIMIT 1
            ) 
            ELSE null 
        END) AS event_name,

        -- Extract notable_event_description
        (CASE 
            WHEN jsonb_typeof(activitydata) <> 'object' 
            AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(activitydata) AS message
                WHERE (message->>'Key')::varchar = 'NotableEventDescription'
            )
            THEN (
                SELECT message->>'Value' 
                FROM jsonb_array_elements(activitydata) AS message
                WHERE (message->>'Key')::varchar = 'NotableEventDescription' 
                LIMIT 1
            ) 
            ELSE null 
        END) AS notable_event_description,

        -- Extract previous_stage
        (CASE 
            WHEN jsonb_typeof(activitydata) <> 'object' 
            AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(activitydata) AS message
                WHERE (message->>'Key')::varchar = 'PreviousStage'
            )
            THEN (
                SELECT message->>'Value' 
                FROM jsonb_array_elements(activitydata) AS message
                WHERE (message->>'Key')::varchar = 'PreviousStage' 
                LIMIT 1
            ) 
            ELSE null 
        END) AS previous_stage,

        -- Extract current_stage
        (CASE 
            WHEN jsonb_typeof(activitydata) <> 'object' 
            AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(activitydata) AS message
                WHERE (message->>'Key')::varchar = 'CurrentStage'
            )
            THEN (
                SELECT message->>'Value' 
                FROM jsonb_array_elements(activitydata) AS message
                WHERE (message->>'Key')::varchar = 'CurrentStage' 
                LIMIT 1
            ) 
            ELSE null 
        END) AS current_stage,

        -- Extract call_type
        (CASE 
            WHEN jsonb_typeof(activitydata) <> 'object' 
            AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(activitydata) AS message
                WHERE (message->>'Key')::varchar = 'CallType'
            )
            THEN (
                SELECT message->>'Value' 
                FROM jsonb_array_elements(activitydata) AS message
                WHERE (message->>'Key')::varchar = 'CallType' 
                LIMIT 1
            ) 
            ELSE null 
        END) AS call_type,

        -- Extract caller
        (CASE 
            WHEN jsonb_typeof(activitydata) <> 'object' 
            AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(activitydata) AS message
                WHERE (message->>'Key')::varchar = 'Caller'
            )
            THEN (
                SELECT message->>'Value' 
                FROM jsonb_array_elements(activitydata) AS message
                WHERE (message->>'Key')::varchar = 'Caller' 
                LIMIT 1
            ) 
            ELSE null 
        END) AS caller,

        -- Extract duration
        (CASE 
            WHEN jsonb_typeof(activitydata) <> 'object' 
            AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(activitydata) AS message
                WHERE (message->>'Key')::varchar = 'Duration'
            )
            THEN (
                SELECT message->>'Value' 
                FROM jsonb_array_elements(activitydata) AS message
                WHERE (message->>'Key')::varchar = 'Duration' 
                LIMIT 1
            ) 
            ELSE null 
        END) AS duration,

        -- Extract call_notes
        (CASE 
            WHEN jsonb_typeof(activitydata) <> 'object' 
            AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(activitydata) AS message
                WHERE (message->>'Key')::varchar = 'CallNotes'
            )
            THEN (
                SELECT message->>'Value' 
                FROM jsonb_array_elements(activitydata) AS message
                WHERE (message->>'Key')::varchar = 'CallNotes' 
                LIMIT 1
            ) 
            ELSE null 
        END) AS call_notes,

        -- Extract previous_owner
        (CASE 
            WHEN jsonb_typeof(activitydata) <> 'object' 
            AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(activitydata) AS message
                WHERE (message->>'Key')::varchar = 'PreviousOwner'
            )
            THEN (
                SELECT message->>'Value' 
                FROM jsonb_array_elements(activitydata) AS message
                WHERE (message->>'Key')::varchar = 'PreviousOwner' 
                LIMIT 1
            ) 
            ELSE null 
        END) AS previous_owner,

        -- Extract current_owner with fallback
        COALESCE(
            (CASE 
                WHEN jsonb_typeof(activitydata) <> 'object' 
                AND EXISTS (
                    SELECT 1 FROM jsonb_array_elements(activitydata) AS message
                    WHERE (message->>'Key')::varchar = 'CurrentOwner'
                )
                THEN (
                    SELECT message->>'Value' 
                    FROM jsonb_array_elements(activitydata) AS message
                    WHERE (message->>'Key')::varchar = 'CurrentOwner' 
                    LIMIT 1
                ) 
                ELSE null 
            END),
            lla.fallback_current_owner
        ) AS current_owner,

        -- Extract has_attachments
        (CASE 
            WHEN jsonb_typeof(activitydata) <> 'object' 
            AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(activitydata) AS message
                WHERE (message->>'Key')::varchar = 'HasAttachments'
            )
            THEN (
                SELECT message->>'Value' 
                FROM jsonb_array_elements(activitydata) AS message
                WHERE (message->>'Key')::varchar = 'HasAttachments' 
                LIMIT 1
            ) 
            ELSE null 
        END) AS has_attachments,

        -- Extract custom fields (mx_custom_1 to mx_custom_17)
        (CASE 
            WHEN jsonb_typeof(activitycustomfields) <> 'object' 
            AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(activitycustomfields) AS message 
                WHERE (message->>'Key')::varchar = 'mx_Custom_1'
            )
            THEN (
                SELECT message->>'Value' 
                FROM jsonb_array_elements(activitycustomfields) AS message
                WHERE (message->>'Key')::varchar = 'mx_Custom_1' 
                LIMIT 1
            )
            ELSE null 
        END) AS mx_custom_1,

        (CASE 
            WHEN jsonb_typeof(activitycustomfields) <> 'object' 
            AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(activitycustomfields) AS message 
                WHERE (message->>'Key')::varchar = 'mx_Custom_2'
            )
            THEN (
                SELECT message->>'Value' 
                FROM jsonb_array_elements(activitycustomfields) AS message
                WHERE (message->>'Key')::varchar = 'mx_Custom_2' 
                LIMIT 1
            )
            ELSE null 
        END) AS mx_custom_2,

        (CASE 
            WHEN jsonb_typeof(activitycustomfields) <> 'object' 
            AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(activitycustomfields) AS message 
                WHERE (message->>'Key')::varchar = 'Status'
            )
            THEN (
                SELECT message->>'Value' 
                FROM jsonb_array_elements(activitycustomfields) AS message
                WHERE (message->>'Key')::varchar = 'Status' 
                LIMIT 1
            )
            ELSE null 
        END) AS mx_custom_status,

        (CASE 
            WHEN jsonb_typeof(activitycustomfields) <> 'object' 
            AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(activitycustomfields) AS message 
                WHERE (message->>'Key')::varchar = 'mx_Custom_3'
            )
            THEN (
                SELECT message->>'Value' 
                FROM jsonb_array_elements(activitycustomfields) AS message
                WHERE (message->>'Key')::varchar = 'mx_Custom_3' 
                LIMIT 1
            )
            ELSE null 
        END) AS mx_custom_3,

        (CASE 
            WHEN jsonb_typeof(activitycustomfields) <> 'object' 
            AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(activitycustomfields) AS message 
                WHERE (message->>'Key')::varchar = 'mx_Custom_4'
            )
            THEN (
                SELECT message->>'Value' 
                FROM jsonb_array_elements(activitycustomfields) AS message
                WHERE (message->>'Key')::varchar = 'mx_Custom_4' 
                LIMIT 1
            )
            ELSE null 
        END) AS mx_custom_4,

        (CASE 
            WHEN jsonb_typeof(activitycustomfields) <> 'object' 
            AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(activitycustomfields) AS message 
                WHERE (message->>'Key')::varchar = 'mx_Custom_5'
            )
            THEN (
                SELECT message->>'Value' 
                FROM jsonb_array_elements(activitycustomfields) AS message
                WHERE (message->>'Key')::varchar = 'mx_Custom_5' 
                LIMIT 1
            )
            ELSE null 
        END) AS mx_custom_5,

        (CASE 
            WHEN jsonb_typeof(activitycustomfields) <> 'object' 
            AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(activitycustomfields) AS message 
                WHERE (message->>'Key')::varchar = 'mx_Custom_6'
            )
            THEN (
                SELECT message->>'Value' 
                FROM jsonb_array_elements(activitycustomfields) AS message
                WHERE (message->>'Key')::varchar = 'mx_Custom_6' 
                LIMIT 1
            )
            ELSE null 
        END) AS mx_custom_6,

        (CASE 
            WHEN jsonb_typeof(activitycustomfields) <> 'object' 
            AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(activitycustomfields) AS message 
                WHERE (message->>'Key')::varchar = 'mx_Custom_7'
            )
            THEN (
                SELECT message->>'Value' 
                FROM jsonb_array_elements(activitycustomfields) AS message
                WHERE (message->>'Key')::varchar = 'mx_Custom_7' 
                LIMIT 1
            )
            ELSE null 
        END) AS mx_custom_7,

        (CASE 
            WHEN jsonb_typeof(activitycustomfields) <> 'object' 
            AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(activitycustomfields) AS message 
                WHERE (message->>'Key')::varchar = 'mx_Custom_8'
            )
            THEN (
                SELECT message->>'Value' 
                FROM jsonb_array_elements(activitycustomfields) AS message
                WHERE (message->>'Key')::varchar = 'mx_Custom_8' 
                LIMIT 1
            )
            ELSE null 
        END) AS mx_custom_8,

        (CASE 
            WHEN jsonb_typeof(activitycustomfields) <> 'object' 
            AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(activitycustomfields) AS message 
                WHERE (message->>'Key')::varchar = 'mx_Custom_9'
            )
            THEN (
                SELECT message->>'Value' 
                FROM jsonb_array_elements(activitycustomfields) AS message
                WHERE (message->>'Key')::varchar = 'mx_Custom_9' 
                LIMIT 1
            )
            ELSE null 
        END) AS mx_custom_9,

        (CASE 
            WHEN jsonb_typeof(activitycustomfields) <> 'object' 
            AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(activitycustomfields) AS message 
                WHERE (message->>'Key')::varchar = 'mx_Custom_10'
            )
            THEN (
                SELECT message->>'Value' 
                FROM jsonb_array_elements(activitycustomfields) AS message
                WHERE (message->>'Key')::varchar = 'mx_Custom_10' 
                LIMIT 1
            )
            ELSE null 
        END) AS mx_custom_10,

        (CASE 
            WHEN jsonb_typeof(activitycustomfields) <> 'object' 
            AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(activitycustomfields) AS message 
                WHERE (message->>'Key')::varchar = 'mx_Custom_11'
            )
            THEN (
                SELECT message->>'Value' 
                FROM jsonb_array_elements(activitycustomfields) AS message
                WHERE (message->>'Key')::varchar = 'mx_Custom_11' 
                LIMIT 1
            )
            ELSE null 
        END) AS mx_custom_11,

        (CASE 
            WHEN jsonb_typeof(activitycustomfields) <> 'object' 
            AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(activitycustomfields) AS message 
                WHERE (message->>'Key')::varchar = 'mx_Custom_12'
            )
            THEN (
                SELECT message->>'Value' 
                FROM jsonb_array_elements(activitycustomfields) AS message
                WHERE (message->>'Key')::varchar = 'mx_Custom_12' 
                LIMIT 1
            )
            ELSE null 
        END) AS mx_custom_12,

        (CASE 
            WHEN jsonb_typeof(activitycustomfields) <> 'object' 
            AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(activitycustomfields) AS message 
                WHERE (message->>'Key')::varchar = 'mx_Custom_13'
            )
            THEN (
                SELECT message->>'Value' 
                FROM jsonb_array_elements(activitycustomfields) AS message
                WHERE (message->>'Key')::varchar = 'mx_Custom_13' 
                LIMIT 1
            )
            ELSE null 
        END) AS mx_custom_13,

        (CASE 
            WHEN jsonb_typeof(activitycustomfields) <> 'object' 
            AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(activitycustomfields) AS message 
                WHERE (message->>'Key')::varchar = 'mx_Custom_14'
            )
            THEN (
                SELECT message->>'Value' 
                FROM jsonb_array_elements(activitycustomfields) AS message
                WHERE (message->>'Key')::varchar = 'mx_Custom_14' 
                LIMIT 1
            )
            ELSE null 
        END) AS mx_custom_14,

        (CASE 
            WHEN jsonb_typeof(activitycustomfields) <> 'object' 
            AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(activitycustomfields) AS message 
                WHERE (message->>'Key')::varchar = 'mx_Custom_15'
            )
            THEN (
                SELECT message->>'Value' 
                FROM jsonb_array_elements(activitycustomfields) AS message
                WHERE (message->>'Key')::varchar = 'mx_Custom_15' 
                LIMIT 1
            )
            ELSE null 
        END) AS mx_custom_15,

        (CASE 
            WHEN jsonb_typeof(activitycustomfields) <> 'object' 
            AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(activitycustomfields) AS message 
                WHERE (message->>'Key')::varchar = 'mx_Custom_16'
            )
            THEN (
                SELECT message->>'Value' 
                FROM jsonb_array_elements(activitycustomfields) AS message
                WHERE (message->>'Key')::varchar = 'mx_Custom_16' 
                LIMIT 1
            )
            ELSE null 
        END) AS mx_custom_16,

        (CASE 
            WHEN jsonb_typeof(activitycustomfields) <> 'object' 
            AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(activitycustomfields) AS message 
                WHERE (message->>'Key')::varchar = 'mx_Custom_17'
            )
            THEN (
                SELECT message->>'Value' 
                FROM jsonb_array_elements(activitycustomfields) AS message
                WHERE (message->>'Key')::varchar = 'mx_Custom_17' 
                LIMIT 1
            )
            ELSE null 
        END) AS mx_custom_17,

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

    FROM recent_activities l
    LEFT JOIN leads_data l2 ON l.relatedprospectid = l2.prospectid 
    LEFT JOIN latest_lead_assigned lla 
        ON lla.relatedprospectid = l.relatedprospectid 
        AND lla.activity_modified_on = l.createdon::timestamp + INTERVAL '5 hours 30 minutes'
    ORDER BY 2, 5;
    """


# DAG definition with params
dag = DAG(
        'LSQ_Leads_and_activities_v2_backfill',
        default_args=default_args,
        description='An Analytics Data Layer DAG for Leads and their activities. Data Source = Leadsquared',
        schedule=None,
        catchup=False,
        params={
                'start_timestamp': '',  # Format: 'YYYY-MM-DD HH:MI:SS'
                'end_timestamp': ''  # Format: 'YYYY-MM-DD HH:MI:SS'
        }
)

# Task 1: Create table if not exists
create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=CREATE_TABLE_SQL,
        dag=dag
)

# Task 2: Transform data with parameterized date filter
transform_data = PostgresOperator(
        task_id='transform_data',
        postgres_conn_id=POSTGRES_LSQ_CONN_ID,
        sql=get_transform_sql(),
        dag=dag
)

# Task 3: Extract data to nested
extract_python_data = PythonOperator(
        task_id='extract_python_data',
        python_callable=extract_data_to_nested,
        provide_context=True,
        dag=dag
)

# Define Task Dependencies
create_table >> transform_data >> extract_python_data