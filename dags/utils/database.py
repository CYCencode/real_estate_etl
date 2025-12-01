#utils/database.py
"""資料庫連線模組"""

import os
import time
import traceback
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from airflow.exceptions import AirflowException

from utils.logging import log_to_gcs


def get_pg_engine_with_retry(max_retries=3, **context):
    """建立 PostgreSQL 連線，支援重試機制"""
    DB_USER = os.environ.get("PG_USER", "postgres")
    DB_PASSWORD = os.environ.get("PG_PASSWORD")
    DB_HOST = os.environ.get("PG_HOST")
    DB_PORT = os.environ.get("PG_PORT", "5432")
    DB_NAME = os.environ.get("PG_DATABASE", "postgres")
    
    if not DB_PASSWORD or not DB_HOST:
        raise ValueError("PG_PASSWORD or PG_HOST environment variable not set")
    
    DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    
    for attempt in range(1, max_retries + 1):
        try:
            log_to_gcs('INFO', f"Attempt {attempt}/{max_retries}: Connecting to PostgreSQL...", 
                      details={"host": DB_HOST, "database": DB_NAME})
            
            engine = create_engine(DATABASE_URL, echo=False)
            
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
                log_to_gcs('INFO', "Successfully connected to PostgreSQL", 
                          details={"attempt": attempt})
                return engine
                
        except OperationalError as e:
            log_to_gcs('WARNING', f"Connection failed (attempt {attempt})", 
                      details={"error": str(e)})
            
            if attempt < max_retries:
                wait_time = 2 ** attempt
                log_to_gcs('INFO', f"Waiting {wait_time}s before retry...")
                time.sleep(wait_time)
            else:
                error_msg = f"Failed to connect to PostgreSQL after {max_retries} attempts"
                log_to_gcs('ERROR', error_msg, details={"error": str(e), "traceback": traceback.format_exc()})
                
            if context and 'task_instance' in context:
                context['task_instance'].xcom_push(key='error_info', value=error_msg)
                context['task_instance'].xcom_push(key='error_type', value='PostgreSQL Connection Error')
                
            raise AirflowException(error_msg)