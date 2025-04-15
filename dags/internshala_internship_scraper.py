from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

from job_scrapers.internships.internshala import InternShalaInternshipScraper

# Constants
MAX_PAGES_PER_ROLE = 4
POSTGRES_CONN_ID = "postgres_job_posting"


@dag(
        dag_id='internshala_internship_scraper_dag',
        description='Scrape and store internships from InternShala',
        schedule='0 8 * * *',
        start_date=datetime(2025, 4, 14),
        catchup=False,
        default_args={
                'owner': 'airflow',
                'depends_on_past': False,
                'email_on_failure': False,
                'email_on_retry': False,
                'retries': 1,
                'retry_delay': timedelta(minutes=5),
        },
        tags=['internshala', 'job-scraping'],
)
def internshala_internship_scraper():
    @task
    def create_table():
        """Create internships table if it doesn't exist"""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql = """
        CREATE TABLE IF NOT EXISTS internship_openings (
            id SERIAL PRIMARY KEY,
            job_id VARCHAR(255) UNIQUE NOT NULL,
            title VARCHAR(255) NOT NULL,
            company VARCHAR(255) NOT NULL,
            locations TEXT[],
            job_description_url TEXT,
            min_monthly_stipend INTEGER,
            max_monthly_stipend INTEGER,
            required_skills TEXT[],
            duration VARCHAR(100),
            start_date VARCHAR(100),
            apply_by VARCHAR(100),
            source VARCHAR(50) NOT NULL,
            inserted_at TIMESTAMP DEFAULT NOW()
        );
        """
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
            conn.commit()

    @task
    def scrape_and_save_internships():
        """Scrape internships from InternShala and save to PostgreSQL database."""
        scraper = InternShalaInternshipScraper(max_retries=3, retry_delay=5)
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        JOB_ROLES = Variable.get(
                "INTERNSHALA_INTERNSHIP_JOB_ROLES",
                default_var=[],
                deserialize_json=True
        )

        for job_role in JOB_ROLES:
            internship_count = 0

            for batch in scraper.fetch_all_internships(job_role, max_pages=MAX_PAGES_PER_ROLE):
                if not batch:
                    continue

                # Prepare values for insertion
                values_list = []
                for internship in batch:
                    values = (
                            internship.job_id,
                            internship.title,
                            internship.company,
                            internship.locations,
                            internship.job_description_url,
                            internship.min_monthly_stipend,
                            internship.max_monthly_stipend,
                            internship.required_skills,
                            internship.duration,
                            internship.start_date,
                            internship.apply_by,
                            internship.source,
                            datetime.now()
                    )
                    values_list.append(values)

                # Use ON CONFLICT DO NOTHING to skip duplicates
                insert_sql = """
                INSERT INTO internship_openings
                (job_id, title, company, locations, job_description_url,
                min_monthly_stipend, max_monthly_stipend, required_skills,
                duration, start_date, apply_by, source, inserted_at)
                VALUES %s
                ON CONFLICT (job_id) DO NOTHING
                """

                # Execute batch insert with execute_values
                with pg_hook.get_conn() as conn:
                    with conn.cursor() as cursor:
                        execute_values(cursor, insert_sql, values_list)
                        conn.commit()
                        internship_count += len(batch)

    # Define task dependencies
    create_table_task = create_table()
    scrape_task = scrape_and_save_internships()

    create_table_task >> scrape_task


# Instantiate the DAG
internshala_dag = internshala_internship_scraper()