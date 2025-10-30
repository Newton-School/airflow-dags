"""
Airflow DAG for fetching LeadSquare activities and storing them in PostgreSQL.

This DAG:
1. Fetches activities from LeadSquare API
2. Maps the API response fields to database columns
3. Converts Data and Fields arrays to JSONB
4. Stores data in lsq_leads_activity_v2 table
"""
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any

import pendulum
from airflow.decorators import dag, task
from airflow.models import Param, Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from leadsquare.client import LSQClient

# Configuration constants
POSTGRES_CONN_ID = "postgres_lsq_leads"
RATE_LIMIT_DELAY = 5  # seconds between API calls

logger = logging.getLogger(__name__)


class LSQActivitiesManager:
    """Manager class for LSQ activities operations."""

    def __init__(self, pg_hook: PostgresHook, lsq_client: LSQClient):
        """Initialize manager with database hook and LSQ client.

        Args:
            pg_hook: PostgreSQL hook for database operations
            lsq_client: LSQ client for API calls
        """
        self.pg_hook = pg_hook
        self.lsq_client = lsq_client

    def create_table(self) -> None:
        """Create lsq_leads_activity_v2 table if it doesn't exist."""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS lsq_leads_activity_v2 (
            type TEXT,
            createdon TEXT,
            eventcode DOUBLE PRECISION,
            eventname TEXT,
            sessionid TEXT,
            activityid TEXT PRIMARY KEY,
            modifiedon TEXT,
            activitydata JSONB,
            activitytype DOUBLE PRECISION,
            activityscore DOUBLE PRECISION,
            relatedprospectid TEXT,
            activitycustomfields JSONB,
            airflow_created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            airflow_modified_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_lsq_activities_v2_createdon ON lsq_leads_activity_v2(createdon);
        CREATE INDEX IF NOT EXISTS idx_lsq_activities_v2_modifiedon ON lsq_leads_activity_v2(modifiedon);
        CREATE INDEX IF NOT EXISTS idx_lsq_activities_v2_relatedprospectid ON lsq_leads_activity_v2(relatedprospectid);
        CREATE INDEX IF NOT EXISTS idx_lsq_activities_v2_eventcode ON lsq_leads_activity_v2(eventcode);
        """

        with self.pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(create_table_query)
                conn.commit()

        logger.info("Table lsq_leads_activity_v2 created or verified")

    def fetch_and_store_activities(self, from_date: str, to_date: str) -> int:
        """Fetch activities from LSQ API and store in database.

        Args:
            from_date: Start date in format 'YYYY-MM-DD HH:MM:SS'
            to_date: End date in format 'YYYY-MM-DD HH:MM:SS'

        Returns:
            Total number of activities processed
        """
        total_activities = 0
        page_index = 1

        while True:
            try:
                logger.info(f"Fetching page {page_index} for date range {from_date} to {to_date}")

                response = self.lsq_client.fetch_activities(
                    from_date=from_date,
                    to_date=to_date,
                    page_index=page_index
                )

                record_count = response.get('RecordCount', 0)
                logger.info(f"Page {page_index}: {record_count} activities found")

                activities = response.get('ProspectActivities', [])
                if not activities:
                    logger.info("No more activities to fetch")
                    break

                # Transform and store activities
                self._store_activities(activities)
                total_activities += len(activities)

                logger.info(f"Stored {len(activities)} activities. Total: {total_activities}")

                # Rate limiting
                time.sleep(RATE_LIMIT_DELAY)
                page_index += 1

            except Exception as e:
                logger.error(f"Error fetching page {page_index}: {str(e)}")
                raise

        return total_activities

    def _store_activities(self, activities: List[Dict]) -> None:
        """Store activities in database with upsert logic.

        Args:
            activities: List of activity dictionaries from API
        """
        with self.pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                for activity in activities:
                    activity_data = self._transform_activity(activity)
                    if activity_data and activity_data.get('activityid'):
                        self._upsert_activity(cursor, activity_data)
                conn.commit()

    def _transform_activity(self, activity: Dict) -> Dict[str, Any]:
        """Transform API activity data to database format.

        Args:
            activity: Activity dictionary from API

        Returns:
            Dictionary with database column names and properly formatted values
        """
        # Convert Data array to JSONB object
        data_array = activity.get('Data', [])
        activity_data_json = {}
        if data_array:
            for item in data_array:
                key = item.get('Key')
                value = item.get('Value')
                if key:
                    activity_data_json[key] = value

        # Convert Fields array to JSONB object
        fields_array = activity.get('Fields', [])
        custom_fields_json = {}
        if fields_array:
            for item in fields_array:
                key = item.get('Key')
                value = item.get('Value')
                if key:
                    custom_fields_json[key] = value

        transformed = {
            'activityid': activity.get('Id'),
            'eventcode': activity.get('EventCode'),
            'eventname': activity.get('EventName'),
            'activityscore': activity.get('ActivityScore'),
            'createdon': activity.get('CreatedOn'),
            'modifiedon': activity.get('ModifiedOn'),
            'activitytype': activity.get('ActivityType'),
            'type': activity.get('Type'),
            'relatedprospectid': activity.get('RelatedProspectId'),
            'sessionid': activity.get('SessionId'),
            'activitydata': json.dumps(activity_data_json) if activity_data_json else None,
            'activitycustomfields': json.dumps(custom_fields_json) if custom_fields_json else None
        }

        return transformed

    def _upsert_activity(self, cursor, activity_data: Dict[str, Any]) -> None:
        """Upsert an activity into the database.

        Args:
            cursor: Database cursor
            activity_data: Dictionary with column names and values
        """
        columns = list(activity_data.keys())
        values = [activity_data[col] for col in columns]

        # Build the INSERT ... ON CONFLICT query
        column_names = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))

        # For the ON CONFLICT UPDATE clause, exclude the primary key
        update_columns = [col for col in columns if col != 'activityid']
        update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])
        # Add airflow_modified_on to update on conflict
        update_clause += ', airflow_modified_on = CURRENT_TIMESTAMP'

        query = f"""
            INSERT INTO lsq_leads_activity_v2 ({column_names}, airflow_created_on, airflow_modified_on)
            VALUES ({placeholders}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (activityid)
            DO UPDATE SET {update_clause}
        """

        cursor.execute(query, values)


@dag(
    dag_id="fetch_lsq_activities_dag",
    schedule="*/15 * * * *",  # Run every 15 minutes
    start_date=pendulum.datetime(2025, 10, 30, tz="UTC"),
    catchup=False,
    tags=["lsq", "activities", "data_ingestion"],
    default_args={
        "owner": "data_team",
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
    },
    params={
        "start_time": Param(
            None,
            type=["null", "string"],
            description="Start time in format 'YYYY-MM-DD HH:MM:SS'. If not provided, fetches from 2 hours ago."
        ),
        "end_time": Param(
            None,
            type=["null", "string"],
            description="End time in format 'YYYY-MM-DD HH:MM:SS'. If not provided, uses current time."
        ),
    },
    doc_md="""
    # LeadSquare Activities Ingestion DAG

    This DAG fetches activities from LeadSquare API and stores them in the lsq_leads_activity_v2 table.

    - Runs every 15 minutes
    - By default, fetches activities modified in the last 2 hours
    - Can override time range using start_time and end_time params
    - Uses upsert logic to handle duplicate activities

    ## Parameters:
    - start_time: Optional start datetime (format: 'YYYY-MM-DD HH:MM:SS')
    - end_time: Optional end datetime (format: 'YYYY-MM-DD HH:MM:SS')
    """,
)
def fetch_lsq_activities_dag():
    """DAG for ingesting LSQ activities."""

    @task(task_id="create_table")
    def create_table() -> bool:
        """Create lsq_leads_activity_v2 table if it doesn't exist."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # Get LSQ credentials from Airflow Variables (all caps)
        lsq_host = Variable.get("LSQ_HOST")
        lsq_access_key = Variable.get("LSQ_ACCESS_KEY")
        lsq_secret_key = Variable.get("LSQ_SECRET_KEY")

        lsq_client = LSQClient(lsq_host, lsq_access_key, lsq_secret_key)
        manager = LSQActivitiesManager(pg_hook, lsq_client)
        manager.create_table()

        return True

    @task(task_id="fetch_and_store_activities")
    def fetch_and_store_activities(table_created: bool, **context) -> Dict[str, Any]:
        """Fetch activities from LSQ and store in database."""
        if not table_created:
            raise ValueError("Table creation failed")

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # Get LSQ credentials from Airflow Variables (all caps)
        lsq_host = Variable.get("LSQ_HOST")
        lsq_access_key = Variable.get("LSQ_ACCESS_KEY")
        lsq_secret_key = Variable.get("LSQ_SECRET_KEY")

        lsq_client = LSQClient(lsq_host, lsq_access_key, lsq_secret_key)
        manager = LSQActivitiesManager(pg_hook, lsq_client)

        # Get params
        params = context["params"]
        start_time_param = params.get("start_time")
        end_time_param = params.get("end_time")

        # Calculate time range
        if start_time_param and end_time_param:
            # Use provided params
            from_date_str = start_time_param
            to_date_str = end_time_param
            logger.info(f"Using provided time range: {from_date_str} to {to_date_str}")
        else:
            # Default: last 2 hours
            current_time = datetime.now(timezone.utc)
            to_date = current_time.replace(second=0, microsecond=0)
            from_date = to_date - timedelta(hours=2)

            from_date_str = from_date.strftime('%Y-%m-%d %H:%M:%S')
            to_date_str = to_date.strftime('%Y-%m-%d %H:%M:%S')
            logger.info(f"Using default time range (last 2 hours): {from_date_str} to {to_date_str}")

        total_activities = manager.fetch_and_store_activities(from_date_str, to_date_str)

        return {
            "total_activities": total_activities,
            "from_date": from_date_str,
            "to_date": to_date_str
        }

    # Define task dependencies
    table_created = create_table()
    result = fetch_and_store_activities(table_created)

    return result


# Instantiate the DAG
fetch_lsq_activities_dag_instance = fetch_lsq_activities_dag()
