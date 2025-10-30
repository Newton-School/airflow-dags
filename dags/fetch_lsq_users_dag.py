"""
Airflow DAG for fetching LeadSquare users and storing them in PostgreSQL.

This DAG:
1. Fetches all users from LeadSquare API
2. Maps the API response fields to database columns
3. Stores data in lsq_users_v2 table
"""
import json
import logging
from typing import Dict, List, Any

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from leadsquare.client import LSQClient

# Configuration constants
POSTGRES_CONN_ID = "postgres_lsq_leads"

logger = logging.getLogger(__name__)


class LSQUsersManager:
    """Manager class for LSQ users operations."""

    def __init__(self, pg_hook: PostgresHook, lsq_client: LSQClient):
        """Initialize manager with database hook and LSQ client.

        Args:
            pg_hook: PostgreSQL hook for database operations
            lsq_client: LSQ client for API calls
        """
        self.pg_hook = pg_hook
        self.lsq_client = lsq_client

    def create_table(self) -> None:
        """Create lsq_users_v2 table if it doesn't exist."""
        create_table_query = """
                             CREATE TABLE IF NOT EXISTS lsq_users_v2
                             (
                                 tag
                                 TEXT,
                                 role
                                 TEXT,
                                 userid
                                 TEXT
                                 PRIMARY
                                 KEY,
                                 lastname
                                 TEXT,
                                 firstname
                                 TEXT,
                                 statuscode
                                 DOUBLE
                                 PRECISION,
                                 emailaddress
                                 TEXT,
                                 isphonecallagent
                                 BOOLEAN,
                                 memberofgroups
                                 JSONB,
                                 airflow_created_on
                                 TIMESTAMP
                                 DEFAULT
                                 CURRENT_TIMESTAMP,
                                 airflow_modified_on
                                 TIMESTAMP
                                 DEFAULT
                                 CURRENT_TIMESTAMP
                             );

                             CREATE INDEX IF NOT EXISTS idx_lsq_users_v2_emailaddress ON lsq_users_v2(emailaddress);
                             CREATE INDEX IF NOT EXISTS idx_lsq_users_v2_role ON lsq_users_v2(role); \
                             """

        with self.pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(create_table_query)
                conn.commit()

        logger.info("Table lsq_users_v2 created or verified")

    def fetch_and_store_users(self) -> int:
        """Fetch users from LSQ API and store in database.

        Returns:
            Total number of users processed
        """
        try:
            logger.info("Fetching all users from LSQ")
            users = self.lsq_client.fetch_users()

            if not users:
                logger.info("No users found")
                return 0

            logger.info(f"Fetched {len(users)} users")

            # Transform and store users
            self._store_users(users)
            logger.info(f"Stored {len(users)} users")

            return len(users)

        except Exception as e:
            logger.error(f"Error fetching users: {str(e)}")
            raise

    def _store_users(self, users: List[Dict]) -> None:
        """Store users in database with upsert logic.

        Args:
            users: List of user dictionaries from API
        """
        with self.pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                for user in users:
                    user_data = self._transform_user(user)
                    if user_data and user_data.get('userid'):
                        self._upsert_user(cursor, user_data)
                conn.commit()

    def _transform_user(self, user: Dict) -> Dict[str, Any]:
        """Transform API user data to database format.

        Args:
            user: User dictionary from API

        Returns:
            Dictionary with database column names and properly formatted values
        """
        # Convert MemberOfGroups array to JSONB
        member_of_groups = user.get('MemberOfGroups', [])

        transformed = {
                'userid': user.get('ID'),
                'firstname': user.get('FirstName'),
                'lastname': user.get('LastName'),
                'emailaddress': user.get('EmailAddress'),
                'role': user.get('Role'),
                'statuscode': user.get('StatusCode'),
                'tag': user.get('Tag'),
                'isphonecallagent': user.get('IsPhoneCallAgent', False),
                'memberofgroups': json.dumps(member_of_groups) if member_of_groups else None
        }

        return transformed

    def _upsert_user(self, cursor, user_data: Dict[str, Any]) -> None:
        """Upsert a user into the database.

        Args:
            cursor: Database cursor
            user_data: Dictionary with column names and values
        """
        columns = list(user_data.keys())
        values = [user_data[col] for col in columns]

        # Build the INSERT ... ON CONFLICT query
        column_names = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))

        # For the ON CONFLICT UPDATE clause, exclude the primary key
        update_columns = [col for col in columns if col != 'userid']
        update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])
        # Add airflow_modified_on to update on conflict
        update_clause += ', airflow_modified_on = CURRENT_TIMESTAMP'

        query = f"""
            INSERT INTO lsq_users_v2 ({column_names}, airflow_created_on, airflow_modified_on)
            VALUES ({placeholders}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (userid)
            DO UPDATE SET {update_clause}
        """

        cursor.execute(query, values)


@dag(
        dag_id="fetch_lsq_users_dag",
        schedule="0 2 * * *",  # Run daily at 2 AM
        start_date=pendulum.datetime(2025, 10, 30, tz="UTC"),
        catchup=False,
        max_active_runs=2,
        tags=["lsq", "users", "data_ingestion"],
        default_args={
                "owner": "data_team",
                "retries": 2,
                "retry_delay": pendulum.duration(minutes=5),
        },
        doc_md="""
    # LeadSquare Users Ingestion DAG

    This DAG fetches all users from LeadSquare API and stores them in the lsq_users_v2 table.

    - Runs daily at 2 AM
    - Fetches all users (full refresh)
    - Uses upsert logic to handle updates
    - Stores MemberOfGroups as JSONB
    """,
)
def fetch_lsq_users_dag():
    """DAG for ingesting LSQ users."""

    @task(task_id="create_table")
    def create_table() -> bool:
        """Create lsq_users_v2 table if it doesn't exist."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # Get LSQ credentials from Airflow Variables (all caps)
        lsq_host = Variable.get("LSQ_HOST")
        lsq_access_key = Variable.get("LSQ_ACCESS_KEY")
        lsq_secret_key = Variable.get("LSQ_SECRET_KEY")

        lsq_client = LSQClient(lsq_host, lsq_access_key, lsq_secret_key)
        manager = LSQUsersManager(pg_hook, lsq_client)
        manager.create_table()

        return True

    @task(task_id="fetch_and_store_users")
    def fetch_and_store_users(table_created: bool) -> Dict[str, Any]:
        """Fetch users from LSQ and store in database."""
        if not table_created:
            raise ValueError("Table creation failed")

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # Get LSQ credentials from Airflow Variables (all caps)
        lsq_host = Variable.get("LSQ_HOST")
        lsq_access_key = Variable.get("LSQ_ACCESS_KEY")
        lsq_secret_key = Variable.get("LSQ_SECRET_KEY")

        lsq_client = LSQClient(lsq_host, lsq_access_key, lsq_secret_key)
        manager = LSQUsersManager(pg_hook, lsq_client)

        total_users = manager.fetch_and_store_users()

        return {
                "total_users": total_users
        }

    # Define task dependencies
    table_created = create_table()
    result = fetch_and_store_users(table_created)

    return result


# Instantiate the DAG
fetch_lsq_users_dag_instance = fetch_lsq_users_dag()
