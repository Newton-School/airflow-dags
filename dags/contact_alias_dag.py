"""
Airflow DAG for managing contact aliases.
"""
import logging

import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from contact_alias.manager import ContactAliasManager

# Configuration constants
RESULT_DATABASE_CONNECTION_ID = "postgres_result_db"
NEWTON_PROD_READ_REPLICA_CONNECTION_ID = "postgres_read_replica"
BACK_FILL = False
logger = logging.getLogger(__name__)


@dag(
        dag_id="contact_alias_dag",
        schedule="27 21 * * *",  # Run at 9:27 PM UTC every day
        start_date=pendulum.datetime(2025, 4, 22, tz="UTC"),
        catchup=False,
        tags=["contact_alias", "data_processing"],
        default_args={
                "owner": "data_team",
                "retries": 3,
                "retry_delay": pendulum.duration(minutes=5),
        },
        doc_md="""
    # Contact Alias DAG

    This DAG manages contact aliases from multiple data sources:

    1. Creates/verifies the contact_aliases table
    2. Processes user data from auth_user table
    3. Processes form responses data

    When in back_fill mode, it processes all historical data.
    Otherwise, it only processes data from yesterday.
    """,
)
def contact_alias_dag():
    """DAG for managing contact aliases from multiple data sources."""

    @task(task_id="create_table")
    def create_table() -> bool:
        """Create the contact aliases table if it doesn't exist."""
        result_db_hook = PostgresHook(postgres_conn_id=RESULT_DATABASE_CONNECTION_ID)
        source_db_hook = PostgresHook(postgres_conn_id=NEWTON_PROD_READ_REPLICA_CONNECTION_ID)
        manager = ContactAliasManager(result_db_hook, source_db_hook, BACK_FILL)
        manager.create_contact_alias_table()
        return True

    @task(task_id="process_auth_user_data")
    def process_auth_user_data(table_created: bool) -> bool:
        """Process data from auth_user table."""
        if not table_created:
            raise ValueError("Table creation task failed")

        result_db_hook = PostgresHook(postgres_conn_id=RESULT_DATABASE_CONNECTION_ID)
        source_db_hook = PostgresHook(postgres_conn_id=NEWTON_PROD_READ_REPLICA_CONNECTION_ID)
        manager = ContactAliasManager(result_db_hook, source_db_hook, BACK_FILL)
        manager.process_auth_user_data()
        return True

    @task(task_id="process_form_responses")
    def process_form_responses(auth_user_processed: bool) -> bool:
        """Process data from generic form responses."""
        if not auth_user_processed:
            raise ValueError("Auth user processing task failed")

        result_db_hook = PostgresHook(postgres_conn_id=RESULT_DATABASE_CONNECTION_ID)
        source_db_hook = PostgresHook(postgres_conn_id=NEWTON_PROD_READ_REPLICA_CONNECTION_ID)
        manager = ContactAliasManager(result_db_hook, source_db_hook, BACK_FILL)
        manager.process_form_response_data()
        return True

    # Define the task dependencies
    table_created = create_table()
    auth_user_processed = process_auth_user_data(table_created)
    form_responses_processed = process_form_responses(auth_user_processed)

    # Return final task for potential downstream dependencies
    return form_responses_processed


@dag(
        dag_id="contact_alias_backfill_dag",
        schedule=None,  # Manual trigger only
        start_date=pendulum.datetime(2025, 4, 23, tz="UTC"),
        catchup=False,
        tags=["contact_alias", "data_processing", "backfill"],
        default_args={
                "owner": "data_team",
                "retries": 0
        },
        params={
                "start_id": {"type": "integer", "default": 0},
                "end_id": {"type": "integer", "default": 10000},
                "source_type": {"type": "string", "default": "AUTH_USER"}
        },
        doc_md="""
    # Contact Alias Backfill DAG

    This DAG processes contact aliases in specific ID ranges:

    1. Creates/verifies the contact_aliases table if needed
    2. Processes records from the specified source within the given ID range

    ## Parameters:
    - start_id: Starting ID (inclusive)
    - end_id: Ending ID (inclusive)
    - source_type: Either 'AUTH_USER' or 'FORM_RESPONSE'

    ## Usage:
    Run this DAG multiple times with different ID ranges to complete the backfill.
    If a range fails, only that specific range needs to be rerun.
    """,
)
def contact_alias_backfill_dag():
    """DAG for backfilling contact aliases in ID ranges."""

    @task(task_id="create_table")
    def create_table() -> bool:
        """Create the contact aliases table if it doesn't exist."""
        result_db_hook = PostgresHook(postgres_conn_id=RESULT_DATABASE_CONNECTION_ID)
        source_db_hook = PostgresHook(postgres_conn_id=NEWTON_PROD_READ_REPLICA_CONNECTION_ID)
        manager = ContactAliasManager(result_db_hook, source_db_hook, True)
        manager.create_contact_alias_table()
        return True

    @task(task_id="process_id_range")
    def process_id_range(table_created: bool, **context) -> bool:
        """Process a specific ID range from a source."""
        if not table_created:
            raise ValueError("Table creation task failed")

        # Get parameters from the DAG run configuration
        dag_run = context["dag_run"]
        conf = dag_run.conf if dag_run and dag_run.conf else {}

        start_id = int(conf.get("start_id", 0))
        end_id = int(conf.get("end_id", 100000))
        source_type = str(conf.get("source_type", "AUTH_USER"))

        logger.info(f"Processing {source_type} records from ID {start_id} to {end_id}")

        result_db_hook = PostgresHook(postgres_conn_id=RESULT_DATABASE_CONNECTION_ID)
        source_db_hook = PostgresHook(postgres_conn_id=NEWTON_PROD_READ_REPLICA_CONNECTION_ID)
        manager = ContactAliasManager(result_db_hook, source_db_hook, True)

        # Process the range
        manager.process_data_by_id_range(source_type, start_id, end_id)
        return True

    # Define the task dependencies
    table_created = create_table()
    id_range_processed = process_id_range(table_created)

    # Return final task for potential downstream dependencies
    return id_range_processed


# Instantiate the DAG
contact_alias_dag_instance = contact_alias_dag()
contact_alias_backfill_dag_instance = contact_alias_backfill_dag()
