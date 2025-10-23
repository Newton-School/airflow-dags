import requests
from datetime import datetime, timedelta
from typing import List, Dict, Any
import pytz

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.runo_utils import store_call_logs_data

POSTGRES_CONN_ID = "postgres_result_db"
RUNO_API_BASE_URL = "https://api.runo.in/v1"
RUNO_SECRET_KEY = Variable.get("RUNO_API_SECRET_KEY", "jZyYmg2NjV5aG41YjU1Mm4=")

@dag(
    dag_id="runo_api_fetcher_by_date_dag",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['runo', 'api-fetching', 'date-based'],
    max_active_runs=1,
    params={
        "start_date": (datetime.now(pytz.timezone('Asia/Kolkata')) - timedelta(days=1)).strftime("%Y-%m-%d"),
        "end_date": (datetime.now(pytz.timezone('Asia/Kolkata')) - timedelta(days=1)).strftime("%Y-%m-%d")
    },
    doc_md="""
    # Runo API Data Fetcher by Date DAG
    
    This DAG fetches call logs data from Runo API for a specific date with pagination support.
    
    ## Parameters:
    - **start_date**: Start date in YYYY-MM-DD format (default: yesterday in IST)
    - **end_date**: End date in YYYY-MM-DD format (default: yesterday in IST)
    
    ## Features:
    - Fetches all pages of data for all dates between start_date and end_date (inclusive)
    - Handles pagination automatically (up to 100 entries per page)
    - Stores data in PostgreSQL using existing table structure
    - Manual trigger only - no scheduled runs
    - Uses IST timezone for date calculations
    
    ## Usage:
    Trigger manually from Airflow UI and provide the desired date range parameters.
    Example: {"start_date": "2025-01-01", "end_date": "2025-01-31"}
    
    ## Validation:
    - Validates that dates are in YYYY-MM-DD format
    - Ensures end_date is not before start_date
    """
)
def runo_api_fetcher_by_date_dag():
    
    @task
    def create_tables():
        """Create necessary database tables if they don't exist"""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        tables = [
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
    def fetch_call_logs_by_date_range_with_pagination(**context) -> List[Dict[str, Any]]:
        """Fetch call logs data from Runo API for a date range with pagination"""
        
        # Get date parameters from context
        start_date = context.get("params", {}).get("start_date")
        end_date = context.get("params", {}).get("end_date")
        
        if not start_date or not end_date:
            raise ValueError("Both start_date and end_date parameters are required. Please provide dates in YYYY-MM-DD format.")
        
        # Validate date formats
        try:
            start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
            end_datetime = datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError as e:
            raise ValueError(f"Invalid date format. Expected YYYY-MM-DD format. Error: {str(e)}")
        
        # Validate date range
        if start_datetime > end_datetime:
            raise ValueError(f"start_date ({start_date}) cannot be after end_date ({end_date})")
        
        # Generate list of dates to process
        date_list = []
        current_date = start_datetime
        while current_date <= end_datetime:
            date_list.append(current_date.strftime("%Y-%m-%d"))
            current_date += timedelta(days=1)
        
        print(f"Processing {len(date_list)} dates from {start_date} to {end_date}")
        
        headers = {
            'Auth-Key': RUNO_SECRET_KEY,
            'Content-Type': 'application/json'
        }
        
        all_call_logs = []
        total_dates_processed = 0
        
        # Process each date
        for target_date in date_list:
            print(f"\n=== Processing date: {target_date} ===")
            
            page_no = 1
            total_entries_fetched_for_date = 0
            max_entries_per_page = 100
            date_call_logs = []
            
            while True:
                try:
                    # Make API request
                    url = f"{RUNO_API_BASE_URL}/call/logs?date={target_date}&pageNo={page_no}"
                    print(f"Fetching page {page_no} for {target_date}: {url}")
                    
                    response = requests.get(
                        url,
                        headers=headers,
                        timeout=30
                    )
                    
                    response.raise_for_status()
                    data = response.json()
                    
                    # Check API response status
                    if data.get('statusCode') != 0:
                        error_msg = data.get('message', 'Unknown error')
                        print(f"API returned error on page {page_no} for {target_date}: {error_msg}")
                        break
                    
                    # Extract data and metadata
                    page_data = data.get('data', {}).get('data', [])
                    metadata = data.get('data', {}).get('metadata', {})
                    
                    total_entries = metadata[0].get('total', 0)
                    current_page = metadata[0].get('page', page_no)
                    
                    print(f"Page {current_page} for {target_date}: Fetched {len(page_data)} entries")
                    print(f"Metadata - Total entries: {total_entries}, Current page: {current_page}")
                    
                    # Add page data to our collection for this date
                    date_call_logs.extend(page_data)
                    total_entries_fetched_for_date += len(page_data)
                    
                    # Check if we should continue pagination
                    if len(page_data) < max_entries_per_page:
                        print(f"Last page reached for {target_date} (returned {len(page_data)} entries, less than {max_entries_per_page})")
                        break
                    
                    if total_entries > 0 and total_entries_fetched_for_date >= total_entries:
                        print(f"All entries fetched for {target_date} (fetched: {total_entries_fetched_for_date}, total: {total_entries})")
                        break
                    
                    # Move to next page
                    page_no += 1
                    
                except requests.exceptions.RequestException as e:
                    print(f"Request error on page {page_no} for {target_date}: {str(e)}")
                    raise
                except Exception as e:
                    print(f"Unexpected error on page {page_no} for {target_date}: {str(e)}")
                    raise
            
            print(f"Completed {target_date}: {len(date_call_logs)} entries fetched")
            all_call_logs.extend(date_call_logs)
            total_dates_processed += 1
        
        print(f"\n=== Summary ===")
        print(f"Total dates processed: {total_dates_processed}")
        print(f"Total call logs collected: {len(all_call_logs)}")
        
        return all_call_logs
    
    
    create_tables_task = create_tables()
    fetch_call_logs_task = fetch_call_logs_by_date_range_with_pagination()
    store_call_logs_task = task(store_call_logs_data)(fetch_call_logs_task)
    
    create_tables_task >> fetch_call_logs_task >> store_call_logs_task

runo_api_fetcher_by_date_dag_instance = runo_api_fetcher_by_date_dag()
