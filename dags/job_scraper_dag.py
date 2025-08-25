import json
import requests
from typing import List, Type, Any, Iterator, Dict, Tuple
from datetime import datetime
from urllib.parse import urljoin

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


from job_scrapers.pruner.glassdoor import GlassdoorJobPruner
from job_scrapers.pruner.weekday import WeekdayJobPruner
from job_scrapers.utils import newton_api_request
from job_scrapers.models import RawJobOpening, EmploymentType
from job_scrapers.scrapers.base import BaseJobScraper
from job_scrapers.scrapers.weekday import WeekdayJobScraper
from job_scrapers.scrapers.glassdoor import GlassdoorJobScraper
from job_scrapers.transformers.base import BaseJobTransformer
from job_scrapers.transformers.weekday import WeekdayJobTransformer
from job_scrapers.transformers.glassdoor import GlassdoorJobTransformer

# Constants
BATCH_SIZE = 100
POSTGRES_CONN_ID = "postgres_job_posting"


def create_scraper_dag(
        dag_id: str,
        scraper_class: Type[BaseJobScraper],
        transformer_class: Type[BaseJobTransformer],
        schedule: str = "30 0 * * *",
        scraper_args: Dict[str, Any] = None,
        transformer_args: Dict[str, Any] = None,
        tags: List[str] = None
):
    """
    Factory function to create a job scraper DAG.
    """

    @dag(
            dag_id=dag_id,
            schedule=schedule,
            start_date=datetime(2024, 1, 1),
            catchup=False,
            tags=tags or [],
            max_active_runs=1
    )
    def job_scraper_dag():

        @task
        def create_tables():
            """Create necessary database tables if they don't exist"""
            pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

            tables = [
                    """
                    CREATE TABLE IF NOT EXISTS airflow_companies (
                        slug VARCHAR(255) PRIMARY KEY,
                        name VARCHAR(255) NOT NULL,
                        normalized_names TEXT[] NOT NULL,
                        website VARCHAR(255),
                        logo_url VARCHAR(255),
                        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                    """,
                    """
                    CREATE TABLE IF NOT EXISTS airflow_raw_job_openings (
                        id SERIAL PRIMARY KEY,
                        external_job_id VARCHAR(255) NOT NULL,
                        source_name VARCHAR(255) NOT NULL,
                        raw_data JSONB NOT NULL,
                        is_external_for_job_board BOOLEAN DEFAULT TRUE,
                        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE (external_job_id)
                    )
                    """,
                    """
                    CREATE TABLE IF NOT EXISTS airflow_processed_job_openings (
                        id SERIAL PRIMARY KEY,
                        external_job_id VARCHAR(255) NOT NULL,
                        title VARCHAR(255) NOT NULL,
                        company_slug VARCHAR(255) NOT NULL,
                        role_hash VARCHAR(255) NOT NULL,
                        description TEXT NOT NULL,
                        min_ctc INTEGER,
                        max_ctc INTEGER,
                        city VARCHAR(255),
                        state VARCHAR(255),
                        employment_type INTEGER NOT NULL,
                        min_experience_years INTEGER,
                        max_experience_years INTEGER,
                        skills TEXT[],
                        external_apply_link VARCHAR(255) NOT NULL,
                        newton_sync_status VARCHAR(50) DEFAULT 'PENDING',
                        newton_sync_error TEXT,
                        newton_sync_attempts INTEGER DEFAULT 0,
                        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        expires_at TIMESTAMP,
                        UNIQUE (external_job_id)
                    )
                    """
            ]

            with pg_hook.get_conn() as conn:
                with conn.cursor() as cur:
                    for query in tables:
                        cur.execute(query)

        @task
        def update_tables():
            """
            Update necessary database tables
            """
            pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            with pg_hook.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        ALTER TABLE airflow_processed_job_openings
                        ALTER COLUMN external_apply_link TYPE TEXT
                    """)
                    conn.commit()

        @task
        def scrape_jobs() -> List[int]:
            """Scrape jobs using the provided scraper"""
            pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            if scraper_args:
                job_type = scraper_args.pop('job_type', EmploymentType.FULL_TIME.value)
            scraper = scraper_class.from_airflow_variables(**(scraper_args or {}))
            raw_job_opening_ids = []

            try:
                scraper.setup()
                for batch in scraper.get_jobs(job_type):
                    if not batch:
                        continue

                    with pg_hook.get_conn() as conn:
                        with conn.cursor() as cur:
                            values_template = ','.join(['(%s, %s, %s, %s)'] * len(batch))
                            insert_query = f"""
                                INSERT INTO airflow_raw_job_openings 
                                    (external_job_id, source_name, raw_data, is_external_for_job_board)
                                VALUES {values_template}
                                ON CONFLICT (external_job_id) 
                                DO NOTHING
                                RETURNING id
                            """
                            flat_values = [
                                    item
                                    for job in batch
                                    for item in (
                                            job.external_job_id,
                                            job.source_name,
                                            json.dumps(job.raw_data),
                                            job.is_external_for_job_board
                                    )
                            ]
                            cur.execute(insert_query, flat_values)
                            raw_job_opening_ids.extend([row[0] for row in cur.fetchall()])
                            conn.commit()

            finally:
                scraper.cleanup()

            return raw_job_opening_ids

        def get_jobs_by_ids(pg_hook: PostgresHook, job_ids: List[int], batch_size: int) -> Iterator[List[RawJobOpening]]:
            """Fetch specific jobs by their IDs in batches"""
            for i in range(0, len(job_ids), batch_size):
                batch_ids = job_ids[i:i + batch_size]
                with pg_hook.get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                                """
                                SELECT id, external_job_id, source_name, raw_data, is_external_for_job_board
                                FROM airflow_raw_job_openings
                                WHERE id = ANY(%s)
                                AND is_external_for_job_board = TRUE
                                """,
                                (batch_ids,)
                        )
                        batch = cur.fetchall()

                        if not batch:
                            continue

                        yield [
                                RawJobOpening(
                                        external_job_id=row[1],
                                        source_name=row[2],
                                        raw_data=row[3],
                                        is_external_for_job_board=row[4]
                                )
                                for row in batch
                        ]

        @task
        def process_jobs(raw_job_opening_ids: List[int]):
            """Process jobs using the provided transformer with batch processing"""
            pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            transformer = transformer_class.from_airflow_variables(**(transformer_args or {}))
            failed_jobs = []


            for batch in get_jobs_by_ids(pg_hook, raw_job_opening_ids, BATCH_SIZE):
                transformed_jobs = list(transformer.transform_jobs(iter(batch)))
                jobs_to_process = [job for job in transformed_jobs if job.external_apply_link]

                if not jobs_to_process:
                    continue

                # Batch insert into processed_job_openings
                with pg_hook.get_conn() as conn:
                    with conn.cursor() as cur:
                        values_template = ','.join(['(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'] * len(jobs_to_process))
                        insert_query = f"""
                            INSERT INTO airflow_processed_job_openings (
                                external_job_id, title, company_slug, role_hash, description,
                                min_ctc, max_ctc, city, state, employment_type,
                                min_experience_years, max_experience_years,
                                skills, external_apply_link
                            ) VALUES {values_template}
                            ON CONFLICT (external_job_id) DO NOTHING
                            RETURNING id, external_job_id
                        """
                        flat_values = [
                                item
                                for job in jobs_to_process
                                for item in (
                                        job.external_job_id, job.title[:255], job.company_slug[:255],
                                        job.role_hash, job.description, job.min_ctc,
                                        job.max_ctc, job.city, job.state,
                                        job.employment_type, job.min_experience_years,
                                        job.max_experience_years, job.skills,
                                        job.external_apply_link
                                )
                        ]
                        cur.execute(insert_query, flat_values)
                        inserted_jobs = cur.fetchall()
                        conn.commit()

                # Create mapping of external_job_id to database id and job data
                id_mapping = {row[1]: row[0] for row in inserted_jobs}
                jobs_to_sync = [job for job in jobs_to_process if job.external_job_id in id_mapping]

                # Batch process Newton API requests
                newton_results = []
                for job in jobs_to_sync:
                    payload = {
                            "title": job.title,
                            "placement_role_hash": job.role_hash,
                            "description": job.description,
                            "min_ctc": job.min_ctc,
                            "max_ctc": job.max_ctc,
                            "employment_type": job.employment_type,
                            "external_apply_link": job.external_apply_link,
                            "location": {
                                    "city": job.city,
                                    "state": job.state
                            },
                            "min_experience_years": job.min_experience_years,
                            "max_experience_years": job.max_experience_years,
                            "source": transformer.source
                    }

                    try:
                        request_url = f"/api/v1/job_scrapers/company/s/{job.company_slug}/job_opening/add/"
                        response = newton_api_request(request_url, payload)
                        newton_results.append(
                                {
                                        'external_job_id': id_mapping[job.external_job_id],
                                        'status': 'SYNCED' if response.status_code in [200, 201] else 'FAILED',
                                        'error': str(response.text)[:500] if response.status_code != 201 else None,
                                        'job': job
                                }
                        )
                    except Exception as e:
                        newton_results.append(
                                {
                                        'external_job_id': id_mapping[job.external_job_id],
                                        'status': 'ERROR',
                                        'error': str(e)[:500],
                                        'job': job
                                }
                        )

                # Batch update Newton sync status
                if newton_results:
                    with pg_hook.get_conn() as conn:
                        with conn.cursor() as cur:
                            update_template = ','.join(['(%s::varchar, %s::varchar, %s::text)'] * len(newton_results))
                            update_query = f"""
                                UPDATE airflow_processed_job_openings AS p
                                SET 
                                    newton_sync_status = c.status,
                                    newton_sync_error = c.error,
                                    newton_sync_attempts = newton_sync_attempts + 1
                                FROM (VALUES {update_template}) AS c(external_job_id, status, error)
                                WHERE p.external_job_id = c.external_job_id::varchar
                            """
                            flat_update_values = [
                                    item
                                    for result in newton_results
                                    for item in (
                                            result['external_job_id'],
                                            result['status'],
                                            result['error']
                                    )
                            ]
                            cur.execute(update_query, flat_update_values)
                            conn.commit()

                # Track failed jobs
                failed_jobs.extend(
                        [
                                result['job']
                                for result in newton_results
                                if result['status'] in ('FAILED', 'ERROR')
                        ]
                )

            if failed_jobs:
                failed_count = len(failed_jobs)
                failed_companies = [job.company_slug for job in failed_jobs]
                print(f"Failed to process {failed_count} jobs for companies: {failed_companies}")

        # Define the task dependencies
        create_tables_task = create_tables()
        update_tables_task = update_tables()
        scrape_task = scrape_jobs()
        process_task = process_jobs(scrape_task)

        create_tables_task >> update_tables_task >> scrape_task >> process_task

    return job_scraper_dag()


@dag(
        dag_id="remove_expired_job_openings",
        schedule_interval="0 0 * * *",
        start_date=datetime(2025, 1, 9),
        catchup=False,
        tags=['job-scraping']
)
def remove_expired_job_openings_dag():
    glassdoor_job_pruner = GlassdoorJobPruner()
    weekday_job_pruner = WeekdayJobPruner()

    @task
    def update_table() -> None:
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                        """
                                            ALTER TABLE airflow_processed_job_openings
                                            ADD COLUMN IF NOT EXISTS expires_at TIMESTAMP
                                        """
                )
                conn.commit()

    @task
    def remove_expired_job_openings() -> None:
        def fetch_jobs(cursor, offset: int, batch_size: int) -> List[Tuple[int, str, str]]:
            query = """
                SELECT airflow_processed_job_openings.id, airflow_processed_job_openings.external_job_id, external_apply_link, source_name
                FROM airflow_processed_job_openings
                LEFT JOIN airflow_raw_job_openings
                ON airflow_processed_job_openings.external_job_id = airflow_raw_job_openings.external_job_id
                WHERE expires_at IS NULL
                AND airflow_processed_job_openings.created_at < NOW() - INTERVAL '30 days'
                ORDER BY id
                LIMIT %s OFFSET %s
            """
            cursor.execute(query, (batch_size, offset))
            return cursor.fetchall()

        def mark_job_expired(cursor, job_id: int) -> None:
            update_query = """
                UPDATE airflow_processed_job_openings 
                SET expires_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """
            cursor.execute(update_query, (job_id,))

        def process_job(job: Tuple[int, str, str, str], cursor) -> None:
            job_id, external_job_id, external_apply_link, source_name = job
            if not external_apply_link:
                return

            try:
                should_prune = False
                if source_name == 'glassdoor':
                    should_prune = glassdoor_job_pruner.should_prune(external_job_id)
                elif source_name == 'weekday':
                    should_prune = weekday_job_pruner.should_prune(external_apply_link)
                if should_prune:
                    request_url = '/api/v1/job_scrapers/job_opening/expire/'
                    payload = {'external_apply_link': external_apply_link}
                    newton_api_request(request_url, payload)
                    mark_job_expired(cursor, job_id)
            except Exception as e:
                print(f"Error while checking job opening {external_job_id}: {str(e)}")

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM airflow_processed_job_openings")
                total_jobs = cur.fetchone()[0]

                for offset in range(0, total_jobs, BATCH_SIZE):
                    job_openings = fetch_jobs(cur, offset, BATCH_SIZE)
                    for job in job_openings:
                        process_job(job, cur)

                    conn.commit()

    update_table_task = update_table()
    remove_expired_job_openings_task = remove_expired_job_openings()

    update_table_task >> remove_expired_job_openings_task


weekday_dag = create_scraper_dag(
        dag_id="scrape_and_transform_weekday_job_openings",
        scraper_class=WeekdayJobScraper,
        transformer_class=WeekdayJobTransformer,
        tags=['weekday', 'job-scraping']
)

glassdoor_dag = create_scraper_dag(
        dag_id="scrape_and_transform_glassdoor_job_openings",
        scraper_class=GlassdoorJobScraper,
        transformer_class=GlassdoorJobTransformer,
        schedule="30 23 * * *",
        tags=['glassdoor', 'job-scraping'],
)

glassdoor_internship_dag = create_scraper_dag(
        dag_id="scrape_and_transform_glassdoor_internship_openings",
        scraper_class=GlassdoorJobScraper,
        transformer_class=GlassdoorJobTransformer,
        schedule="30 0 * * *",
        scraper_args={'job_type': EmploymentType.INTERNSHIP.value},
        tags=['glassdoor', 'internship', 'job-scraping'],
)

expired_job_openings_dag = remove_expired_job_openings_dag()
