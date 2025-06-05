import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from psycopg2.extras import execute_values

from course_x_user_info.etl import load_last_7_days


@dag(
    dag_id="course_x_user_info",
    start_date=pendulum.datetime(2025, 5, 27, tz="UTC"),
    schedule=None,
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
        sql="sql/prepare_schema.sql",
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
        rows = [(slug, bl) for slug, bl in mapping.items()]

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
        load_last_7_days(fetch_batch=5_000, insert_batch=1_000)

    # ------------------------------------------------------------------ #
    # Task dependencies                                                  #
    # ------------------------------------------------------------------ #
    prepare_schema >> sync_bl_map() >> load()


# expose DAG object to Airflow
dag = course_x_user_info()
