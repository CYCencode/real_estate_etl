#dags/real_estate_here_geocoding
"""
房地產地址轉經緯度 DAG
從 PostgreSQL 讀取房地產資料，使用 HERE API 將地址轉換為經緯度座標
並將結果寫回資料庫
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.exceptions import AirflowException
from datetime import timedelta
import time
import os
import traceback

from config.settings import default_args, CONTROL_VM_IP, TABLES
from utils import log_to_gcs, get_pg_engine_with_retry,send_slack_success_notification, send_slack_failure_notification
from pipelines.here_geocoding import process_table_geocoding


# ==================== Airflow Tasks ====================

def geocoding_pipeline(**context):
    """地址轉經緯度主要流程"""
    start_time = time.time()
    
    try:
        log_to_gcs('INFO', "開始地址轉經緯度流程")
        
        # 取得 HERE API Key
        api_key = os.environ.get('HERE_API_KEY')
        if not api_key:
            raise ValueError("HERE_API_KEY environment variable not set")
        
        log_to_gcs('INFO', "HERE API Key 已載入")
        
        # 建立 PostgreSQL 連線
        engine = get_pg_engine_with_retry(**context)
        log_to_gcs('INFO', "PostgreSQL 連線建立成功")
        
        # 處理所有資料表
        all_results = []
        total_success = 0
        total_failed = 0
        total_records = 0
        
        for table_name in TABLES:
            result = process_table_geocoding(
                table_name=table_name,
                api_key=api_key,
                engine=engine,
                batch_size=200,        
                batch_delay=1.0,      
                request_delay=0.2     # 每個請求延遲 0.2 秒 (5 req/sec)
            )
            all_results.append(result)
            
            if result['status'] == 'completed':
                total_records += result['total']
                total_success += result['success']
                total_failed += result['failed']
        
        end_time = time.time()
        duration = end_time - start_time
        
        # 準備統計資訊
        summary = {
            'total_tables': len(TABLES),
            'total_records': total_records,
            'total_success': total_success,
            'total_failed': total_failed,
            'success_rate': f"{total_success/total_records*100:.1f}%" if total_records > 0 else "0%",
            'duration': f"{duration:.2f}",
            'results': all_results
        }
        
        log_to_gcs('INFO', "地址轉經緯度流程完成", details=summary)
        
        # 推送結果到 XCom
        context['task_instance'].xcom_push(key='summary', value=summary)
        context['task_instance'].xcom_push(key='total_records', value=total_records)
        context['task_instance'].xcom_push(key='total_success', value=total_success)
        context['task_instance'].xcom_push(key='total_failed', value=total_failed)
        context['task_instance'].xcom_push(key='duration', value=f"{duration:.2f}")
        
        return summary
        
    except Exception as e:
        log_to_gcs('CRITICAL', f"地址轉經緯度流程失敗", details={
            "error": str(e),
            "traceback": traceback.format_exc()
        })
        
        context['task_instance'].xcom_push(key='error_info', value=str(e))
        context['task_instance'].xcom_push(key='error_type', value=type(e).__name__)
        
        raise AirflowException(f"Geocoding pipeline failed: {str(e)}")


# ==================== DAG 定義 ====================

with DAG(
    'real_estate_here_geocoding',
    default_args=default_args,
    description='Convert real estate addresses to latitude/longitude coordinates using HERE API',
    schedule_interval='0 4 5 1,4,7,10 *',  # 每季 ETL 後執行 (1/5, 4/5, 7/5, 10/5) 凌晨 4 點
    catchup=False,
    tags=['production', 'geocoding', 'real-estate'],
) as dag:
    
    # Task: 地址轉經緯度主要流程
    geocoding_task = PythonOperator(
        task_id='run_geocoding',
        python_callable=geocoding_pipeline,
        execution_timeout=timedelta(hours=6),
    )
    
    # Task: 成功通知
    notify_success = PythonOperator(
    task_id='notify_success',
    python_callable=send_slack_success_notification,
    trigger_rule='all_success',
    )
    
    # Task: 失敗通知
    notify_failure = PythonOperator(
    task_id='notify_failure',
    python_callable=send_slack_failure_notification,
    trigger_rule='one_failed',
    )
    
    # 設定任務依賴關係
    geocoding_task >> [notify_success, notify_failure]