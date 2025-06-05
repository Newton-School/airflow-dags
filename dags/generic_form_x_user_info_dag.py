import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator

from dags.generic_form_x_user_info.schema import GENERIC_FORM_X_USER_INFO_SCHEMA
from generic_form_x_user_info.etl import load_data


@dag(
    dag_id="generic_form_x_user_info",
    start_date=pendulum.datetime(2025, 6, 5, tz="UTC"),
    schedule=None,
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


# expose DAG object to Airflow
dag = generic_form_x_user_info_dag()
