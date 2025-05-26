"""
This DAG fetches user information from newton_api, combines it with data from contact_alias, and stores it in the 'results' database.
"""

import logging
import uuid
from datetime import timezone
from typing import List, Optional, Tuple, Dict, Any

import pendulum
from airflow.decorators import dag, task
from airflow.models import Param
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Configuration constants
RESULT_DATABASE_CONNECTION_ID = "postgres_result_db"
SOURCE_DATABASE_CONNECTION_ID = "postgres_read_replica"
BATCH_SIZE = 1000  # Process data in batches to avoid memory issues
logger = logging.getLogger(__name__)


@dag(
        dag_id="course_x_user_info",
        schedule=None,
        start_date=pendulum.datetime(2025, 5, 27, tz="UTC"),
        catchup=False,
        default_args={
                "owner": "snehil",
                "retries": 1,
                "retry_delay": pendulum.duration(minutes=5),
        }
)
def course_x_user_info():
        """DAG to fetch user information from newton_api, combine it with contact_alias data, and store it in the results database."""

        @task(task_id="create_table_if_not_exists")
        def create_table_if_not_exists():
            """Create the results table if it does not exist."""
            postgres_hook = PostgresHook(RESULT_DATABASE_CONNECTION_ID)
            create_table_sql = """
            
            """
            postgres_hook.run(create_table_sql)
            logger.info("Results table created or already exists.")
