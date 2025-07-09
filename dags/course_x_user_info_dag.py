import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable, Param
from psycopg2.extras import execute_values

from course_x_user_info.etl import load_course_user_info_data
from course_x_user_info.schema import COURSE_X_USER_INFO_SCHEMA


@dag(
    dag_id="course_x_user_info",
    start_date=pendulum.datetime(2025, 5, 27, tz="UTC"),
    schedule="15 16 * * *",
    catchup=False,
    default_args={
        "owner": "data_team",
        "retries": None
        # "retry_delay": pendulum.duration(minutes=1),
    },
    tags=["etl", "course", "user_info", "business_line_mapping"],
)
def course_x_user_info():
    # ------------------------------------------------------------------ #
    # 1. one-off DDL & trigger install (classic operator)                #
    # ------------------------------------------------------------------ #
    prepare_schema = PostgresOperator(
        task_id="prepare_schema",
        postgres_conn_id="postgres_result_db",
        sql=COURSE_X_USER_INFO_SCHEMA,
    )

    # ------------------------------------------------------------------ #
    # 2. TaskFlow task – keep BL mapping table in sync                    #
    # ------------------------------------------------------------------ #
    @task
    def sync_bl_map():
        mapping = Variable.get(
            "COURSE_STRUCTURE_BUSINESS_LINE_MAPPING",
            deserialize_json=True,
            default_var={},
        )
        if not mapping:
            return

        hook = PostgresHook("postgres_result_db")
        rows = [(slug, bl) for slug, bl in mapping.items() if bl]

        with hook.get_conn() as conn, conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO course_structure_business_line
                       (coursestructure_slug, business_line)
                VALUES %s
                ON CONFLICT (coursestructure_slug)
                DO UPDATE SET business_line = EXCLUDED.business_line;
                """,
                rows,
            )
            conn.commit()

    # ------------------------------------------------------------------ #
    # 3. TaskFlow task – fetch & upsert last-7-days data                  #
    # ------------------------------------------------------------------ #
    @task
    def load():
        load_course_user_info_data(fetch_batch=1_000, insert_batch=1_000)

    # ------------------------------------------------------------------ #
    # Task dependencies                                                  #
    # ------------------------------------------------------------------ #
    prepare_schema >> sync_bl_map() >> load()


@dag(
        dag_id="course_x_user_info_backfill",
        start_date=pendulum.datetime(2025, 5, 27, tz="UTC"),
        schedule=None,
        catchup=False,
        default_args={
                "owner": "data_team",
                "retries": None
                # "retry_delay": pendulum.duration(minutes=1),
        },
        tags=["etl", "course", "user_info", "business_line_mapping", "backfill"],
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
def course_x_user_info_backfill():
    # ------------------------------------------------------------------ #
    # 1. one-off DDL & trigger install (classic operator)                #
    # ------------------------------------------------------------------ #
    prepare_schema = PostgresOperator(
        task_id="prepare_schema",
        postgres_conn_id="postgres_result_db",
        sql=COURSE_X_USER_INFO_SCHEMA,
    )

    # ------------------------------------------------------------------ #
    # 2. TaskFlow task – Sync BL mapping table in sync                   #
    # ------------------------------------------------------------------ #
    @task(task_id="sync_bl_map")
    def sync_bl_map():
        mapping = Variable.get(
            "COURSE_STRUCTURE_BUSINESS_LINE_MAPPING",
            deserialize_json=True,
            default_var={},
        )
        if not mapping:
            return

        hook = PostgresHook("postgres_result_db")
        rows = [(slug, bl) for slug, bl in mapping.items() if bl]

        with hook.get_conn() as conn, conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO course_structure_business_line
                       (coursestructure_slug, business_line)
                VALUES %s
                ON CONFLICT (coursestructure_slug)
                DO UPDATE SET business_line = EXCLUDED.business_line;
                """,
                rows,
            )
            conn.commit()


    # -------------------------------------------------------------------------------- #
    # 3. TaskFlow task – fetch & upsert all data within given date range in parameters #
    # -------------------------------------------------------------------------------- #
    @task(task_id="load_data")
    def load_data(**context):
        params = context["params"]
        start_datetime = params.get("start_datetime")
        end_datetime = params.get("end_datetime")
        if not start_datetime or not end_datetime:
            raise ValueError("Both start_datetime and end_datetime must be provided for backfill.")

        load_course_user_info_data(fetch_batch=1_000, insert_batch=1_000, start_datetime=start_datetime, end_datetime=end_datetime)

    # ------------------------------------------------------------------ #
    # Task dependencies                                                  #
    # ------------------------------------------------------------------ #
    prepare_schema >> sync_bl_map() >> load_data()


# expose DAG object to Airflow
dag = course_x_user_info()
dag_backfill = course_x_user_info_backfill()
