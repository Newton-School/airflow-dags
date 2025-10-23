"""
Shared utilities for Runo API DAGs
"""
from datetime import datetime, timezone, date, time
import pytz
from typing import List, Dict, Any, Tuple

from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = "postgres_result_db"
IST_TIMEZONE = pytz.timezone('Asia/Kolkata')


def convert_utc_timestamp_to_ist(utc_timestamp: float) -> Tuple[date, time]:
    """
    Convert UTC timestamp to IST date and time.
    
    Args:
        utc_timestamp: Unix timestamp in UTC
        
    Returns:
        Tuple of (date, time) in IST timezone
    """
    # Convert UTC timestamp to datetime object
    utc_datetime = datetime.fromtimestamp(utc_timestamp, tz=timezone.utc)
    
    # Convert to IST
    ist_datetime = utc_datetime.astimezone(IST_TIMEZONE)
    
    # Extract date and time components
    call_date = ist_datetime.date()
    call_time = ist_datetime.time()
    
    return call_date, call_time


def store_call_logs_data(call_logs_data: List[Dict[str, Any]]) -> int:
    """Store call_logs data in PostgreSQL"""
    if not call_logs_data:
        print("No call logs data to store")
        return 0
    
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            values_list = []
            for call_log in call_logs_data:
                call_date, call_time = convert_utc_timestamp_to_ist(call_log['startTime'])
                created_at = datetime.fromtimestamp(call_log['createdAt'])
                
                values = (
                    call_log.get('callId'),
                    call_log.get('callerId'),
                    call_log.get('calledBy'),
                    call_log.get('name'),
                    call_log.get('customerId'),
                    call_log.get('phoneNumber'),
                    call_date,
                    call_time,
                    call_log.get('duration'),
                    call_log.get('type'),
                    call_log.get('status'),
                    call_log.get('tag'),
                    created_at,
                )
                values_list.append(values)
            
            insert_query = """
                INSERT INTO airflow_runo_call_logs 
                (call_id, caller_id, called_by, customer_name, customer_id, 
                 phone_number, call_date, call_time, duration, call_type, status, tag, 
                 created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (call_id) 
                DO UPDATE SET
                    caller_id = EXCLUDED.caller_id,
                    called_by = EXCLUDED.called_by,
                    customer_name = EXCLUDED.customer_name,
                    customer_id = EXCLUDED.customer_id,
                    phone_number = EXCLUDED.phone_number,
                    call_date = EXCLUDED.call_date,
                    call_time = EXCLUDED.call_time,
                    duration = EXCLUDED.duration,
                    call_type = EXCLUDED.call_type,
                    status = EXCLUDED.status,
                    tag = EXCLUDED.tag,
                    created_at = EXCLUDED.created_at
            """
            
            cur.executemany(insert_query, values_list)
            conn.commit()
            
            stored_count = len(values_list)
            print(f"Stored {stored_count} call logs in database")
            return stored_count
