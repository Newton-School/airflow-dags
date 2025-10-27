"""
Runo API Data Fetcher DAGs
Two separate DAGs in a single file for fetching data from Runo API
"""
from datetime import datetime, timedelta
from typing import List, Dict, Any
import pytz

from airflow.decorators import dag, task
from airflow.models import Variable

from runo.api_client import RunoApiClient
from runo.models import RunoDataManager
from runo.constants import POSTGRES_CONN_ID

# Configuration
RUNO_SECRET_KEY = Variable.get("RUNO_API_SECRET_KEY", "jZyYmg2NjV5aG41YjU1Mm4=")

# =============================================================================
# DAG 1: Scheduled Daily Fetcher
# =============================================================================

@dag(
    dag_id="runo_api_fetcher_dag",
    schedule="0 19 * * *",  # Daily at 00:30 IST (19:00 UTC previous day)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['runo', 'api-fetching', 'scheduled'],
    max_active_runs=1,
    doc_md="""
    # Runo API Scheduled Data Fetcher DAG
    
    This DAG automatically fetches the latest data from Runo API on a daily schedule.
    
    ## Features:
    - **Automated Scheduling**: Runs automatically every day at 00:30 IST
    - **Dual Data Sources**: Fetches both callers and call logs data
    - **Database Management**: Creates tables automatically if they don't exist
    - **Error Handling**: Comprehensive error handling and logging
    - **Data Deduplication**: Uses UPSERT operations to prevent duplicates
    
    ## Data Sources:
    - `/v1/user` - Callers/Users information
    - `/v1/call/logs` - Call logs data
    
    ## Database Tables:
    - `airflow_runo_callers` - Stores caller/user information
    - `airflow_runo_call_logs` - Stores call log details
    """
)
def runo_api_fetcher_dag():
    
    @task
    def create_tables():
        """Create necessary database tables if they don't exist"""
        return RunoDataManager.create_all_tables()
    
    @task
    def fetch_and_store_callers() -> int:
        """Fetch callers data from Runo API and store in database"""
        api_client = RunoApiClient(api_key=RUNO_SECRET_KEY)
        
        success, callers_data = api_client.get_callers()
        if not success:
            raise Exception("Failed to fetch callers data from Runo API")
        
        if not callers_data:
            print("No callers data received from API")
            return 0
        
        stored_count = RunoDataManager.process_callers_data(callers_data)
        return stored_count
    
    @task
    def fetch_and_store_call_logs() -> int:
        """Fetch latest call logs data from Runo API and store in database"""
        api_client = RunoApiClient(api_key=RUNO_SECRET_KEY)
        
        print("Scheduled run: Fetching latest call logs")
        success, call_logs_data = api_client.get_call_logs()
        
        if not success:
            raise Exception("Failed to fetch call logs data from Runo API")
        
        if not call_logs_data:
            print("No call logs data received from API")
            return 0
        
        stored_count = RunoDataManager.process_call_logs_data(call_logs_data)
        return stored_count
    
    @task
    def test_api_connection() -> bool:
        """Test API connection"""
        api_client = RunoApiClient(api_key=RUNO_SECRET_KEY)
        return api_client.test_connection()
    
    # Task definitions
    create_tables_task = create_tables()
    test_connection_task = test_api_connection()
    fetch_callers_task = fetch_and_store_callers()
    fetch_call_logs_task = fetch_and_store_call_logs()
    
    # Task dependencies
    create_tables_task >> test_connection_task
    test_connection_task >> [fetch_callers_task, fetch_call_logs_task]

# =============================================================================
# DAG 2: Manual Date Range Fetcher
# =============================================================================

@dag(
    dag_id="runo_api_fetcher_by_date_dag",
    schedule=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['runo', 'api-fetching', 'date-based', 'manual'],
    max_active_runs=1,
    params={
        "start_date": (datetime.now(pytz.timezone('Asia/Kolkata')) - timedelta(days=1)).strftime("%Y-%m-%d"),
        "end_date": (datetime.now(pytz.timezone('Asia/Kolkata')) - timedelta(days=1)).strftime("%Y-%m-%d")
    },
    doc_md="""
    # Runo API Manual Date Range Fetcher DAG
    
    This DAG allows manual fetching of historical data for specific date ranges with advanced pagination support.
    
    ## Features:
    - **Manual Trigger Only**: No automatic scheduling
    - **Date Range Support**: Fetch data for any date range
    - **Advanced Pagination**: Automatically handles pagination (100 entries per page)
    - **IST Timezone**: Uses Indian Standard Time for all date calculations
    - **Flexible Parameters**: Accept custom start and end dates
    - **Bulk Processing**: Processes multiple dates in a single run
    - **Comprehensive Logging**: Detailed progress tracking for each date and page
    
    ## Parameters:
    - **start_date**: Start date in YYYY-MM-DD format (default: yesterday in IST)
    - **end_date**: End date in YYYY-MM-DD format (default: yesterday in IST)
    
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
        return RunoDataManager.create_all_tables()
    
    @task
    def fetch_and_store_call_logs_by_date_range(**context) -> int:
        """Fetch call logs data from Runo API for a date range and store in database"""
        api_client = RunoApiClient(api_key=RUNO_SECRET_KEY)
        
        # Get date parameters from context
        params = context.get("params", {})
        start_date = params.get("start_date")
        end_date = params.get("end_date")
        
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
        
        print(f"Manual run: Fetching call logs from {start_date} to {end_date}")
        
        success, call_logs_data = api_client.get_call_logs_by_date_range(start_date, end_date)
        
        if not success:
            raise Exception("Failed to fetch call logs data from Runo API")
        
        if not call_logs_data:
            print("No call logs data received from API")
            return 0
        
        stored_count = RunoDataManager.process_call_logs_data(call_logs_data)
        return stored_count
    
    @task
    def test_api_connection() -> bool:
        """Test API connection"""
        api_client = RunoApiClient(api_key=RUNO_SECRET_KEY)
        return api_client.test_connection()
    
    # Task definitions
    create_tables_task = create_tables()
    test_connection_task = test_api_connection()
    fetch_call_logs_task = fetch_and_store_call_logs_by_date_range()
    
    # Task dependencies
    create_tables_task >> test_connection_task >> fetch_call_logs_task

# =============================================================================
# Create DAG instances
# =============================================================================

runo_api_fetcher_dag_instance = runo_api_fetcher_dag()
runo_api_fetcher_by_date_dag_instance = runo_api_fetcher_by_date_dag()