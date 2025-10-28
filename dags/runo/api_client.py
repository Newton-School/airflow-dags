"""Runo API Client for fetching call logs and caller data"""
import json
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta

import requests
from requests import RequestException
from requests.exceptions import JSONDecodeError as RequestsJSONDecodeError

from airflow.models import Variable

from .constants import RUNO_API_BASE_URL, RUNO_API_TIMEOUT, ENDPOINTS


class RunoApiClient:
    
    def __init__(
        self,
        api_url: str = RUNO_API_BASE_URL,
        api_key: str = None,
        timeout: int = RUNO_API_TIMEOUT
    ):
        self.__api_url = api_url.rstrip('/')
        self.__api_key = api_key or Variable.get("RUNO_API_SECRET_KEY")
        self.__timeout = timeout
        self.__session = requests.Session()
        self.__session.timeout = timeout
        
        self.__session.headers.update({
            'Auth-Key': self.__api_key,
            'Content-Type': 'application/json'
        })
    
    def __log_error(self, message: str, context: str = "") -> None:
        print(f"Runo API Error {context}: {message}")
    
    def __request(self, method: str, path: str, context: str, **kwargs) -> Tuple[bool, Optional[Dict[str, Any]]]:
        url = f"{self.__api_url}{path}"
        
        try:
            response = self.__session.request(method, url, **kwargs)
        except RequestException as e:
            self.__log_error(f"Request exception: {e}", context)
            return False, None
        
        try:
            response_data = response.json()
        except (json.JSONDecodeError, RequestsJSONDecodeError):
            response_data = {}
        
        if response.ok:
            return True, response_data
        
        # Log API errors
        error_msg = response_data.get('message', 'Unknown error')
        self.__log_error(f"API error: {error_msg}", context)
        return False, response_data
    
    def get_callers(self) -> Tuple[bool, List[Dict[str, Any]]]:
        success, response_data = self.__request(
            method="GET",
            path=ENDPOINTS["USERS"],
            context="while fetching callers data"
        )
        
        if not success:
            return False, []
        
        # Check API response status
        if response_data.get('statusCode') != 0:
            error_msg = response_data.get('message', 'Unknown error')
            self.__log_error(f"API returned error: {error_msg}", "while fetching callers data")
            return False, []
        
        callers = response_data.get('data', [])
        print(f"Fetched {len(callers)} callers from Runo API")
        return True, callers
    
    def get_call_logs(self) -> Tuple[bool, List[Dict[str, Any]]]:
        success, response_data = self.__request(
            method="GET",
            path=ENDPOINTS["CALL_LOGS"],
            context="while fetching call logs data"
        )
        
        if not success:
            return False, []
        
        # Check API response status
        if response_data.get('statusCode') != 0:
            error_msg = response_data.get('message', 'Unknown error')
            self.__log_error(f"API returned error: {error_msg}", "while fetching call logs data")
            return False, []
        
        call_logs = response_data.get('data', [])
        print(f"Fetched {len(call_logs)} call logs from Runo API")
        return True, call_logs
    
    def get_call_logs_by_date(
        self, 
        date: str, 
        page_no: int = 1, 
        max_entries_per_page: int = 100
    ) -> Tuple[bool, List[Dict[str, Any]], Dict[str, Any]]:
        success, response_data = self.__request(
            method="GET",
            path=f"{ENDPOINTS['CALL_LOGS']}?date={date}&pageNo={page_no}",
            context=f"while fetching call logs for date {date}, page {page_no}"
        )
        
        if not success:
            return False, [], {}
        
        # Check API response status
        if response_data.get('statusCode') != 0:
            error_msg = response_data.get('message', 'Unknown error')
            self.__log_error(f"API returned error: {error_msg}", f"while fetching call logs for date {date}")
            return False, [], {}
        
        # Extract data and metadata
        data_section = response_data.get('data', {})
        page_data = data_section.get('data', [])
        metadata = data_section.get('metadata', [])
        
        # Get metadata info
        metadata_info = metadata[0] if metadata else {}
        total_entries = metadata_info.get('total', 0)
        current_page = metadata_info.get('page', page_no)
        
        print(f"Page {current_page} for {date}: Fetched {len(page_data)} entries")
        print(f"Metadata - Total entries: {total_entries}, Current page: {current_page}")
        
        return True, page_data, metadata_info
    
    def get_call_logs_by_date_range(
        self, 
        start_date: str, 
        end_date: str
    ) -> Tuple[bool, List[Dict[str, Any]]]:
        # Validate date formats
        try:
            start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
            end_datetime = datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError as e:
            self.__log_error(f"Invalid date format: {e}", "date range validation")
            return False, []
        
        # Validate date range
        if start_datetime > end_datetime:
            self.__log_error(f"start_date ({start_date}) cannot be after end_date ({end_date})", "date range validation")
            return False, []
        
        # Generate list of dates to process
        date_list = []
        current_date = start_datetime
        while current_date <= end_datetime:
            date_list.append(current_date.strftime("%Y-%m-%d"))
            current_date += timedelta(days=1)
        
        print(f"Processing {len(date_list)} dates from {start_date} to {end_date}")
        
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
                success, page_data, metadata = self.get_call_logs_by_date(
                    date=target_date,
                    page_no=page_no,
                    max_entries_per_page=max_entries_per_page
                )
                
                if not success:
                    print(f"Failed to fetch page {page_no} for {target_date}")
                    break
                
                # Add page data to our collection for this date
                date_call_logs.extend(page_data)
                total_entries_fetched_for_date += len(page_data)
                
                # Check if we should continue pagination
                if len(page_data) < max_entries_per_page:
                    print(f"Last page reached for {target_date} (returned {len(page_data)} entries, less than {max_entries_per_page})")
                    break
                
                total_entries = metadata.get('total', 0)
                if total_entries > 0 and total_entries_fetched_for_date >= total_entries:
                    print(f"All entries fetched for {target_date} (fetched: {total_entries_fetched_for_date}, total: {total_entries})")
                    break
                
                # Move to next page
                page_no += 1
            
            print(f"Completed {target_date}: {len(date_call_logs)} entries fetched")
            all_call_logs.extend(date_call_logs)
            total_dates_processed += 1
        
        print(f"\n=== Summary ===")
        print(f"Total dates processed: {total_dates_processed}")
        print(f"Total call logs collected: {len(all_call_logs)}")
        
        return True, all_call_logs
