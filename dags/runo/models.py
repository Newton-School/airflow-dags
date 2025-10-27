"""Database models and operations for Runo data"""
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timezone, date, time
import pytz

from airflow.providers.postgres.hooks.postgres import PostgresHook

from .constants import POSTGRES_CONN_ID, CALLERS_TABLE_NAME, CALL_LOGS_TABLE_NAME

IST_TIMEZONE = pytz.timezone('Asia/Kolkata')


class RunoCaller:
    
    @staticmethod
    def create_table() -> str:
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {CALLERS_TABLE_NAME} (
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
        """
        
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(create_table_query)
                conn.commit()
        
        return f"Table {CALLERS_TABLE_NAME} created successfully"
    
    @staticmethod
    def transform_data(api_data: List[Dict[str, Any]]) -> List[tuple]:
        transformed_data = []
        
        for caller in api_data:
            transformed_caller = (
                caller.get('userId'),
                caller.get('name'),
                caller.get('phoneNumber'),
                caller.get('email'),
                caller.get('designation'),
                caller.get('process', []),
            )
            transformed_data.append(transformed_caller)
        
        return transformed_data
    
    @staticmethod
    def store_data(transformed_data: List[tuple]) -> int:
        if not transformed_data:
            print("No callers data to store")
            return 0
        
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        insert_query = f"""
            INSERT INTO {CALLERS_TABLE_NAME} 
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
        
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.executemany(insert_query, transformed_data)
                conn.commit()
                
                stored_count = len(transformed_data)
                print(f"Stored {stored_count} callers in database")
                return stored_count


class RunoCallLog:
    
    @staticmethod
    def create_table() -> str:
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {CALL_LOGS_TABLE_NAME} (
                id SERIAL PRIMARY KEY,
                call_id VARCHAR(255) NOT NULL UNIQUE,
                caller_id VARCHAR(255) NOT NULL,
                called_by VARCHAR(255),
                customer_name VARCHAR(255),
                customer_id VARCHAR(255),
                phone_number VARCHAR(20),
                call_timestamp TIMESTAMP,
                duration INTEGER,
                call_type VARCHAR(50),
                status VARCHAR(255),
                tag VARCHAR(255),
                created_at TIMESTAMP,
                inserted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """
        
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(create_table_query)
                conn.commit()
        
        return f"Table {CALL_LOGS_TABLE_NAME} created successfully"
    
    @staticmethod
    def get_ist_timestamp_from_utc_timestamp(utc_timestamp: float) -> datetime:
        utc_datetime = datetime.fromtimestamp(utc_timestamp, tz=timezone.utc)
        ist_datetime = utc_datetime.astimezone(IST_TIMEZONE)
        return ist_datetime.replace(tzinfo=None)

    @staticmethod
    def transform_data(api_data: List[Dict[str, Any]]) -> List[tuple]:
        transformed_data = []
        
        for call_log in api_data:
            call_timestamp = None
            created_at = None
            
            if call_log.get('startTime'):
                try:
                    call_timestamp = RunoCallLog.get_ist_timestamp_from_utc_timestamp(call_log['startTime'])
                except (ValueError, TypeError, AttributeError):
                    print(f"Warning: Could not parse startTime: {call_log.get('startTime')}")
            
            if call_log.get('createdAt'):
                try:
                    created_at = RunoCallLog.get_ist_timestamp_from_utc_timestamp(call_log['createdAt'])
                except (ValueError, TypeError, AttributeError):
                    print(f"Warning: Could not parse createdAt: {call_log.get('createdAt')}")
            
            transformed_call_log = (
                call_log.get('callId'),
                call_log.get('callerId'),
                call_log.get('calledBy'),
                call_log.get('name'),
                call_log.get('customerId'),
                call_log.get('phoneNumber'),
                call_timestamp,
                call_log.get('duration'),
                call_log.get('type'),
                call_log.get('status'),
                call_log.get('tag'),
                created_at,
            )
            transformed_data.append(transformed_call_log)
        
        return transformed_data
    
    @staticmethod
    def store_data(transformed_data: List[tuple]) -> int:
        if not transformed_data:
            print("No call logs data to store")
            return 0
        
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        insert_query = f"""
            INSERT INTO {CALL_LOGS_TABLE_NAME} 
            (call_id, caller_id, called_by, customer_name, customer_id, phone_number, 
             call_timestamp, duration, call_type, status, tag, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (call_id) 
            DO UPDATE SET
                caller_id = EXCLUDED.caller_id,
                called_by = EXCLUDED.called_by,
                customer_name = EXCLUDED.customer_name,
                customer_id = EXCLUDED.customer_id,
                phone_number = EXCLUDED.phone_number,
                call_timestamp = EXCLUDED.call_timestamp,
                duration = EXCLUDED.duration,
                call_type = EXCLUDED.call_type,
                status = EXCLUDED.status,
                tag = EXCLUDED.tag,
                created_at = EXCLUDED.created_at,
                inserted_at = CURRENT_TIMESTAMP
        """
        
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.executemany(insert_query, transformed_data)
                conn.commit()
                
                stored_count = len(transformed_data)
                print(f"Stored {stored_count} call logs in database")
                return stored_count


class RunoDataManager:
    
    @staticmethod
    def create_all_tables() -> str:
        callers_result = RunoCaller.create_table()
        call_logs_result = RunoCallLog.create_table()
        
        return f"{callers_result}\n{call_logs_result}"
    
    @staticmethod
    def process_callers_data(api_data: List[Dict[str, Any]]) -> int:
        transformed_data = RunoCaller.transform_data(api_data)
        return RunoCaller.store_data(transformed_data)
    
    @staticmethod
    def process_call_logs_data(api_data: List[Dict[str, Any]]) -> int:
        transformed_data = RunoCallLog.transform_data(api_data)
        return RunoCallLog.store_data(transformed_data)
