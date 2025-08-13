import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator

from generic_form_x_user_info.schema import GENERIC_FORM_X_USER_INFO_SCHEMA
from generic_form_x_user_info.etl import load_data


@dag(
    dag_id="generic_form_x_user_info",
    start_date=pendulum.datetime(2025, 6, 5, tz="UTC"),
    schedule="25 */1 * * *",
    catchup=False,
    default_args={
        "owner": "data_team",
        "retries": None
        # "retry_delay": pendulum.duration(minutes=1),
    },
    tags=["etl", "generic_form_response", "user_info"],
)
def generic_form_x_user_info_dag():
    # ------------------------------------------------------------------ #
    # 1. one-off DDL & trigger install (classic operator)                #
    # ------------------------------------------------------------------ #
    prepare_schema = PostgresOperator(
        task_id="prepare_schema",
        postgres_conn_id="postgres_result_db",
        sql=GENERIC_FORM_X_USER_INFO_SCHEMA,
    )

    # ------------------------------------------------------------------ #
    # 2. TaskFlow task – fetch & upsert data                             #
    # ------------------------------------------------------------------ #
    @task
    def load():
        load_data(fetch_batch=1_000, insert_batch=1_000)

    # ------------------------------------------------------------------ #
    # Task dependencies                                                  #
    # ------------------------------------------------------------------ #
    prepare_schema >> load()


@dag(
    dag_id="generic_form_x_user_info_backfill",
    start_date=pendulum.datetime(2025, 6, 5, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "data_team",
        "retries": None
        # "retry_delay": pendulum.duration(minutes=1),
    },
    tags=["etl", "generic_form_response", "user_info", "backfill"],
    params={
        "start_datetime": Param(
            default=None,
            type="string",
            format="date-time",
            title="Start Datetime (ISO 8601 format)",
            description="Start datetime for backfill in ISO 8601 format (e.g., '2025-01-01T00:00:00Z')"
        ),
        "end_datetime": Param(
            default=None,
            type="string",
            format="date-time",
            title="End Datetime (ISO 8601 format)",
            description="End datetime for backfill in ISO 8601 format (e.g., '2025-01-07T23:59:59Z')"
        ),
    }
)
def generic_form_x_user_info_backfill_dag():
    # ------------------------------------------------------------------ #
    # 1. one-off DDL & trigger install (classic operator)                #
    # ------------------------------------------------------------------ #
    prepare_schema = PostgresOperator(
        task_id="prepare_schema",
        postgres_conn_id="postgres_result_db",
        sql=GENERIC_FORM_X_USER_INFO_SCHEMA,
    )

    # ------------------------------------------------------------------ #
    # 2. TaskFlow task – fetch & upsert data for custom date range       #
    # ------------------------------------------------------------------ #
    @task(task_id="load_data")
    def load_data(**context):
        params = context["params"]
        start_datetime = params.get("start_datetime")
        end_datetime = params.get("end_datetime")
        if not start_datetime or not end_datetime:
            raise ValueError("Both start_datetime and end_datetime must be provided for backfill.")

        from generic_form_x_user_info.etl import load_data as load_etl_data
        load_etl_data(fetch_batch=1_000, insert_batch=1_000, start_datetime=start_datetime, end_datetime=end_datetime)

    # ------------------------------------------------------------------ #
    # Task dependencies                                                  #
    # ------------------------------------------------------------------ #
    prepare_schema >> load_data()


# expose DAG object to Airflow
dag = generic_form_x_user_info_dag()
dag_backfill = generic_form_x_user_info_backfill_dag()
