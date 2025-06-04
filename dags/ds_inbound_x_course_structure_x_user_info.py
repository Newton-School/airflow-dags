import logging
import pendulum
from typing import List, Dict, Optional, Any
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException
from psycopg2.extras import Json, execute_values

# Configuration constants
RESULT_DATABASE_CONNECTION_ID = "postgres_result_db"
SOURCE_DATABASE_CONNECTION_ID = "postgres_read_replica"
BATCH_SIZE = 1000
FETCH_BATCH_SIZE = 5000  # Batch size for fetching from source
logger = logging.getLogger(__name__)

# Define task dependencies
FETCH_DS_INBOUND_X_COURSE_INFO_QUERY = """
SELECT
    m.user_id,
    m.response_json->>'full_name' AS full_name,
    c.id AS course_structure_id,
    CASE
        WHEN m.response_json->>'email' LIKE '%value%' AND m.response_json->'email'->>'value' IS NOT NULL AND m.response_json->'email'->>'value' <> ''
        THEN m.response_json->'email'->>'value'
        WHEN m.response_json->>'email' IS NOT NULL
        THEN m.response_json->>'email'
        ELSE NULL
    END AS email,
    m.response_json->>'phone_number' AS phone_number,
    m.response_json->>'current_status' AS current_status,
    m.response_json->>'graduation_year' AS graduation_year,
    m.response_json->>'highest_qualification' AS highest_qualification,
    m.response_json->>'degree' AS graduation_degree,
    m.response_json->>'current_role' AS current_job_role,
    m.response_json->>'course_type_interested_in' AS course_type_interested_in,
    m.response_json->'utm_params'->>'course_structure_slug' AS course_structure_slug,
    m.response_json->'utm_params'->>'incoming_course_structure_slug' AS incoming_course_structure_slug,
    m.response_json->'utm_params'->>'utm_source' AS utm_source,
    m.response_json->'utm_params'->>'utm_medium' AS utm_medium,
    m.response_json->'utm_params'->>'utm_campaign' AS utm_campaign,
    m.response_json->>'from' AS from_source,
    m.created_at
FROM marketing_genericformresponse m
LEFT JOIN courses_coursestructure c 
ON COALESCE(
    m.response_json->>'course_type_interested_in',
    m.response_json->'utm_params'->>'course_structure_slug',
    m.response_json->'utm_params'->>'incoming_course_structure_slug'
) = c.slug
WHERE m.created_at >= CAST((NOW() + INTERVAL '-7 day') AS date)
    AND m.created_at < CAST(NOW() AS date);
"""

# Count query for pagination

COUNT_QUERY = """
SELECT COUNT(*)
FROM ds_inbound_form_response_v2
where 
"""


@dag(
    dag_id="ds_inbound_x_course_structure_x_user_info",
    schedule=None,
    start_date=pendulum.datetime(2025, 5, 27, tz="UTC"),
    catchup=False,
    default_args={
        "owner": "data_team",
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=1),
    }
)

def ds_inbound_x_course_structure_x_user_info():
    """Optimized DAG to fetch user information from newton_api and store in results database."""

    @task(task_id="create_tables_if_not_exists")
    def fetch_and_process_course_user_data():
        hook = PostgresHook(SOURCE_DATABASE_CONNECTION_ID)
        data = hook.get_records(FETCH_DS_INBOUND_X_COURSE_INFO_QUERY)
        for record in data:
            process_record(record)

    def process_record(record):
        course_structure_id = get_course_structure_id(record)
        if not course_structure_id:
            return
        further_process(record)

    def further_process(record):
        user_info_id = get_user_info_id(record)
        if not user_info_id:
            return

        course_structure_x_user_info_id = get_course_structure_x_user_info_id(record)
        if course_structure_x_user_info_id:
            update_course_structure_x_user_info_record(course_structure_x_user_info_id, responses, created_at)
        else:
            create_course_structure_x_user_info_record(record)

    def update_course_structure_x_user_info_record(course_structure_x_user_info_id, responses, created_at):
        course_structure_x_user_info_row = get_course_structure_x_user_info(course_structure_x_user_info_id)
        latest_update_at  = course_structure_x_user_info_row[index of latest updated at in sql query]
        if created_at > latest_updated_at:
            update_form_responses(course_structure_x_user_info_id)
        else:
            update_counter()



    def create_course_structure_x_user_info_record():

# Define task dependencies
fetch_data_task = fetch_and_process_course_user_data()

# Instantiate the DAG
ds_inbound_x_course_structure_x_user_info_dag = ds_inbound_x_course_structure_x_user_info()