import requests
from datetime import datetime
from typing import List, Dict, Any

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.runo_utils import store_call_logs_data

POSTGRES_CONN_ID = "postgres_result_db"
RUNO_API_BASE_URL = "https://api.runo.in/v1"
RUNO_SECRET_KEY = Variable.get("RUNO_API_SECRET_KEY", "jZyYmg2NjV5aG41YjU1Mm4=")

@dag(
    dag_id="runo_api_fetcher_dag",
    schedule="0 19 * * *",  # Daily at 00:30 IST (19:00 UTC previous day), this is needed because the API is only available after 8PM IST and before 10AM IST
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['runo', 'api-fetching'],
    max_active_runs=1,
    doc_md="""
    # Runo API Data Fetcher DAG
    
    This DAG fetches data from Runo API endpoints:
    1. Callers data from /v1/user
    2. Call logs data from /v1/call/logs
    
    Data is stored in PostgreSQL tables for further processing.
    """
)
def runo_api_fetcher_dag():
    
    @task
    def create_tables():
        """Create necessary database tables if they don't exist"""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        tables = [
            """
            CREATE TABLE IF NOT EXISTS airflow_runo_callers (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(255) NOT NULL UNIQUE,
                name VARCHAR(255) NOT NULL,
                phone_number VARCHAR(20),
                email VARCHAR(255),
                designation VARCHAR(255),
                processes TEXT[],
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS airflow_runo_call_logs (
                id SERIAL PRIMARY KEY,
                call_id VARCHAR(255) NOT NULL UNIQUE,
                caller_id VARCHAR(255) NOT NULL,
                called_by VARCHAR(255),
                customer_name VARCHAR(255),
                customer_id VARCHAR(255),
                phone_number VARCHAR(20),
                call_date DATE,
                call_time TIME,
                duration INTEGER,
                call_type VARCHAR(50),
                status VARCHAR(255),
                tag VARCHAR(255),
                created_at TIMESTAMP,
                inserted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        ]
        
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                for query in tables:
                    cur.execute(query)
                conn.commit()
        
        return "Tables created successfully"
    
    @task
    def fetch_callers_data() -> List[Dict[str, Any]]:
        """Fetch callers data from Runo API"""
        headers = {
            'Auth-Key': RUNO_SECRET_KEY,
            'Content-Type': 'application/json'
        }
        
        try:
            response = requests.get(
                f"{RUNO_API_BASE_URL}/user",
                headers=headers,
                timeout=30
            )

            try:
                data = response.json()
                message = data.get('message', None)
                if message:
                    print(f"Status Message: {message}")
            except Exception as e:
                print(f"Error fetching callers data: {str(e)}")

            response.raise_for_status()
            
            if data.get('statusCode') == 0:
                callers = data.get('data', [])
                print(f"Fetched {len(callers)} callers from Runo API")
                return callers
            else:
                print(f"API returned error: {data.get('message', 'Unknown error')}")
                return []
                
        except requests.exceptions.RequestException as e:
            print(f"Error fetching callers data: {str(e)}")
            raise
        except Exception as e:
            print(f"Unexpected error: {str(e)}")
            raise
    
    @task
    def fetch_call_logs_data() -> List[Dict[str, Any]]:
        """Fetch call logs data from Runo API"""
        headers = {
            'Auth-Key': RUNO_SECRET_KEY,
            'Content-Type': 'application/json'
        }
        
        try:
            response = requests.get(
                f"{RUNO_API_BASE_URL}/call/logs",
                headers=headers,
                timeout=30
            )

            try:
                data = response.json()
                message = data.get('message', None)
                if message:
                    print(f"Status Message: {message}")
            except Exception as e:
                print(f"Error fetching call logs data: {str(e)}")

            response.raise_for_status()
            
            if data.get('statusCode') == 0:
                call_logs = data.get('data', [])
                print(f"Fetched {len(call_logs)} call logs from Runo API")
                return call_logs
            else:
                print(f"API returned error: {data.get('message', 'Unknown error')}")
                return []
                
        except requests.exceptions.RequestException as e:
            print(f"Error fetching call logs data: {str(e)}")
            raise
        except Exception as e:
            print(f"Unexpected error: {str(e)}")
            raise
    
    @task
    def store_callers_data(callers_data: List[Dict[str, Any]]) -> int:
        """Store callers data in PostgreSQL"""
        if not callers_data:
            print("No callers data to store")
            return 0
        
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                values_list = []
                for caller in callers_data:
                    values = (
                        caller.get('userId'),
                        caller.get('name'),
                        caller.get('phoneNumber'),
                        caller.get('email'),
                        caller.get('designation'),
                        caller.get('process', []),
                    )
                    values_list.append(values)
                
                insert_query = """
                    INSERT INTO airflow_runo_callers 
                    (user_id, name, phone_number, email, designation, processes)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (user_id) 
                    DO UPDATE SET
                        name = EXCLUDED.name,
                        phone_number = EXCLUDED.phone_number,
                        email = EXCLUDED.email,
                        designation = EXCLUDED.designation,
                        processes = EXCLUDED.processes,
                        updated_at = CURRENT_TIMESTAMP
                """
                
                cur.executemany(insert_query, values_list)
                conn.commit()
                
                stored_count = len(values_list)
                print(f"Stored {stored_count} callers in database")
                return stored_count
    
    
    create_tables_task = create_tables()
    
    fetch_callers_task = fetch_callers_data()
    fetch_call_logs_task = fetch_call_logs_data()
    
    store_callers_task = store_callers_data(fetch_callers_task)
    store_call_logs_task = task(store_call_logs_data)(fetch_call_logs_task)
    
    create_tables_task >> [fetch_callers_task, fetch_call_logs_task]
    fetch_callers_task >> store_callers_task
    fetch_call_logs_task >> store_call_logs_task

runo_api_fetcher_dag_instance = runo_api_fetcher_dag()
