#dags/real_estate_etl.py
"""
房地產季度 ETL DAG
從政府開放平台下載實價登錄資料，經過清洗轉換後載入 PostgreSQL
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.exceptions import AirflowException
from datetime import timedelta
import time
import traceback

from config.settings import default_args, CONTROL_VM_IP, AREA_CODE_NAME, TRADE_TYPE_MAPPING
from utils import log_to_gcs, calculate_target_year_season, get_pg_engine_with_retry, send_slack_success_notification,send_slack_failure_notification
from etls import extract_trade_data


# ==================== Airflow Tasks ====================

def calculate_date_task(**context):
    """計算目標年份和季度"""
    execution_date = context['execution_date']
    year, season = calculate_target_year_season(execution_date)
    
    # 推送到 XCom
    context['ti'].xcom_push(key='target_year', value=year)
    context['ti'].xcom_push(key='target_season', value=season)
    
    return {'year': year, 'season': season}


def etl_main_task(**context):
    """主要 ETL 任務"""
    start_time = time.time()
    total_rows = 0
    
    try:
        log_to_gcs('INFO', "Starting Real Estate ETL Pipeline")
        
        # 建立 PostgreSQL 連線
        engine = get_pg_engine_with_retry(**context)
        
        # 從 XCom 獲取年份和季度
        ti = context['ti']
        year = ti.xcom_pull(key='target_year', task_ids='calculate_date')
        season = ti.xcom_pull(key='target_season', task_ids='calculate_date')
        
        log_to_gcs('INFO', f"開始處理 {year}S{season} 不動產資料", details={
            "year": year,
            "season": season
        })
        
        # 執行 ETL - 所有地區和交易類型
        areas = list(AREA_CODE_NAME.keys()) # ['A', 'F', 'H', 'J', 'O']
        trade_types = list(TRADE_TYPE_MAPPING.keys())  # ['A', 'B']
        
        for area in areas:
            for trade_type in trade_types:
                try:
                    rows = extract_trade_data(year, season, area, trade_type, engine, **context)
                    total_rows += rows
                except Exception as e:
                    area_name = AREA_CODE_NAME[area]
                    type_name = TRADE_TYPE_MAPPING[trade_type]
                    log_to_gcs('ERROR', f"Pipeline failed for area={area_name}, type={type_name}",
                              details={"error": str(e)})
        
        end_time = time.time()
        duration = end_time - start_time
        
        context['task_instance'].xcom_push(key='total_rows', value=total_rows)
        context['task_instance'].xcom_push(key='duration', value=f"{duration:.2f}")
        context['task_instance'].xcom_push(key='status', value='success')
        context['task_instance'].xcom_push(key='year', value=year)
        context['task_instance'].xcom_push(key='season', value=season)
        
        log_to_gcs('INFO', f"Pipeline completed successfully. Total rows: {total_rows}, Duration: {duration:.2f}s")
        
        return {
            'status': 'success',
            'total_rows': total_rows,
            'duration': duration
        }
        
    except AirflowException:
        raise
        
    except Exception as e:
        tb = traceback.format_exc()
        log_to_gcs('CRITICAL', f"Pipeline execution failed: {type(e).__name__}",
                  details={"error": str(e), "traceback": tb})
        
        context['task_instance'].xcom_push(key='error_info', value=str(e))
        context['task_instance'].xcom_push(key='error_type', value=type(e).__name__)
        
        raise AirflowException(f"Pipeline failed: {str(e)}")


# ==================== DAG 定義 ====================

with DAG(
    'real_estate_quarterly_etl',
    default_args=default_args,
    description='Real Estate ETL Pipeline - Extract data from government platform and load into PostgreSQL',
    schedule_interval='0 2 5 1,4,7,10 *',  # 每季資料釋出日 (1/5, 4/5, 7/5, 10/5) 凌晨 2 點執行
    catchup=False,
    tags=['production', 'etl', 'real-estate'],
) as dag:
    
    # Task 1: 計算目標年份和季度
    calculate_date = PythonOperator(
        task_id='calculate_date',
        python_callable=calculate_date_task,
    )
    
    # Task 2: ETL 主要處理流程
    etl_task = PythonOperator(
        task_id='run_etl',
        python_callable=etl_main_task,
    )
    
    # Task 3: 成功通知
    notify_success = PythonOperator(
    task_id='notify_success',
    python_callable=send_slack_success_notification,
    trigger_rule='all_success',
    )
    
    # Task 4: PostgreSQL 連線失敗通知
    notify_pg_failure = PythonOperator(
    task_id='notify_pg_failure',
    python_callable=send_slack_failure_notification,
    trigger_rule='one_failed',
    )

    # Task 5: 一般錯誤通知
    notify_general_failure = PythonOperator(
    task_id='notify_general_failure',
    python_callable=send_slack_failure_notification,
    trigger_rule='one_failed',
    )
    
    # 設定任務依賴關係
    calculate_date >> etl_task >> [notify_success, notify_pg_failure, notify_general_failure]