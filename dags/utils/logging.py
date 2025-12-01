#utils/logging.py
"""GCS 日誌模組"""
from datetime import datetime
from google.cloud import storage
import logging
from config import GCS_BUCKET_NAME

def log_to_gcs(log_level: str, message: str, details=None, log_prefix='real_estate', **context):
    """將日誌寫入 GCS 並輸出到 Airflow"""
    
    # 直接使用 Python logging（
    logger = logging.getLogger(__name__)
    
    # 先輸出到 Airflow（確保即時顯示）
    log_method = getattr(logger, log_level.lower(), logger.info)
    log_method(message)
    if details:
        log_method(f"  Details: {details}")
    
    # 再寫入 GCS（非阻塞）
    try:
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET_NAME)
        
        timestamp = datetime.now()
        log_path = f"logs/{log_prefix}/{timestamp.strftime('%Y-%m-%d')}/{timestamp.strftime('%H-%M-%S')}_{log_level}.log"
        
        log_content = f"[{timestamp.isoformat()}] [{log_level}] {message}\n"
        if details:
            log_content += f"Details: {details}\n"
        
        blob = bucket.blob(log_path)
        blob.upload_from_string(log_content, content_type='text/plain')
            
    except Exception as e:
        logger.error(f"[GCS_LOG_FAILED] {message} (GCS Error: {e})")