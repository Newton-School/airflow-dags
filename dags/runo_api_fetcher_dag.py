"""Runo API Data Fetcher DAGs"""
from datetime import datetime, timedelta
from typing import List, Dict, Any
import pytz

from airflow.decorators import dag, task
from airflow.models import Variable

from runo.api_client import RunoApiClient
from runo.models import RunoDataManager
from runo.constants import POSTGRES_CONN_ID

RUNO_SECRET_KEY = Variable.get("RUNO_API_SECRET_KEY")

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
        return RunoDataManager.create_all_tables()
    
    @task
    def fetch_callers_data() -> List[Dict[str, Any]]:
        api_client = RunoApiClient(api_key=RUNO_SECRET_KEY)
        success, callers_data = api_client.get_callers()
        if not success:
            raise Exception("Failed to fetch callers data from Runo API")
        return callers_data
    
    @task
    def store_callers_data(callers_data: List[Dict[str, Any]]) -> int:
        if not callers_data:
            print("No callers data received from API")
            return 0
        return RunoDataManager.process_callers_data(callers_data)
    
    @task
    def fetch_call_logs_data() -> List[Dict[str, Any]]:
        api_client = RunoApiClient(api_key=RUNO_SECRET_KEY)
        print("Scheduled run: Fetching latest call logs")
        success, call_logs_data = api_client.get_call_logs()
        if not success:
            raise Exception("Failed to fetch call logs data from Runo API")
        return call_logs_data
    
    @task
    def store_call_logs_data(call_logs_data: List[Dict[str, Any]]) -> int:
        if not call_logs_data:
            print("No call logs data received from API")
            return 0
        return RunoDataManager.process_call_logs_data(call_logs_data)
    
    create_tables_task = create_tables()
    fetch_callers_task = fetch_callers_data()
    fetch_call_logs_task = fetch_call_logs_data()
    store_callers_task = store_callers_data(fetch_callers_task)
    store_call_logs_task = store_call_logs_data(fetch_call_logs_task)
    
    create_tables_task >> [fetch_callers_task, fetch_call_logs_task]
    fetch_callers_task >> store_callers_task
    fetch_call_logs_task >> store_call_logs_task

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
        return RunoDataManager.create_all_tables()
    
    @task
    def fetch_call_logs_by_date_range(**context) -> List[Dict[str, Any]]:
        api_client = RunoApiClient(api_key=RUNO_SECRET_KEY)
        
        params = context.get("params", {})
        start_date = params.get("start_date")
        end_date = params.get("end_date")
        
        if not start_date or not end_date:
            raise ValueError("Both start_date and end_date parameters are required. Please provide dates in YYYY-MM-DD format.")
        
        try:
            start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
            end_datetime = datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError as e:
            raise ValueError(f"Invalid date format. Expected YYYY-MM-DD format. Error: {str(e)}")
        
        if start_datetime > end_datetime:
            raise ValueError(f"start_date ({start_date}) cannot be after end_date ({end_date})")
        
        print(f"Manual run: Fetching call logs from {start_date} to {end_date}")
        
        success, call_logs_data = api_client.get_call_logs_by_date_range(start_date, end_date)
        if not success:
            raise Exception("Failed to fetch call logs data from Runo API")
        return call_logs_data
    
    @task
    def store_call_logs_data(call_logs_data: List[Dict[str, Any]]) -> int:
        if not call_logs_data:
            print("No call logs data received from API")
            return 0
        return RunoDataManager.process_call_logs_data(call_logs_data)
    
    create_tables_task = create_tables()
    fetch_call_logs_task = fetch_call_logs_by_date_range()
    store_call_logs_task = store_call_logs_data(fetch_call_logs_task)
    
    create_tables_task >> fetch_call_logs_task >> store_call_logs_task

runo_api_fetcher_dag_instance = runo_api_fetcher_dag()
runo_api_fetcher_by_date_dag_instance = runo_api_fetcher_by_date_dag()