"""
Constants and configuration for Runo API integration
"""

# API Configuration
RUNO_API_BASE_URL = "https://api.runo.in/v1"
RUNO_API_TIMEOUT = 30

# Database Configuration
POSTGRES_CONN_ID = "postgres_result_db"

# Table Names
CALLERS_TABLE_NAME = "airflow_runo_callers"
CALL_LOGS_TABLE_NAME = "airflow_runo_call_logs"

# API Endpoints
ENDPOINTS = {
    "USERS": "/user",
    "CALL_LOGS": "/call/logs"
}

# Pagination
MAX_ENTRIES_PER_PAGE = 100
