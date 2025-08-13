"""
Airflow DAG for managing contact aliases.
"""
import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models import Param
from airflow.providers.postgres.hooks.postgres import PostgresHook

from contact_alias.manager import ContactAliasManager

# Configuration constants
RESULT_DATABASE_CONNECTION_ID = "postgres_result_db"
NEWTON_PROD_READ_REPLICA_CONNECTION_ID = "postgres_read_replica"
BACK_FILL = False
logger = logging.getLogger(__name__)


@dag(
        dag_id="contact_alias_dag",
        schedule="5 */1 * * *",  # Run at :05 every hour
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
        start_date=pendulum.datetime(2025, 4, 21, tz="UTC"),
        catchup=False,
        tags=["contact_alias", "data_processing", "backfill"],
        default_args={
                "owner": "data_team",
                "retries": 2,
                "retry_delay": pendulum.duration(minutes=5),
        },
        params={
                "start_id": Param(0, type="integer", minimum=0),
                "end_id": Param(1000, type="integer", minimum=1),
                "source_type": Param("AUTH_USER", enum=["AUTH_USER", "FORM_RESPONSE"])
        },
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
    def process_id_range(table_created: bool, **context):
        """Process a specific ID range from a source."""
        if not table_created:
            raise ValueError("Table creation task failed")

        params = context["params"]
        start_id = params["start_id"]
        end_id = params["end_id"]
        source_type = params["source_type"]

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

    return id_range_processed


# Instantiate the DAG
contact_alias_dag_instance = contact_alias_dag()
contact_alias_backfill_dag_instance = contact_alias_backfill_dag()
