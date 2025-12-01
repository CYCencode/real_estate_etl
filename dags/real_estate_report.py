#dags/real_estate_report
"""
房地產季報 Email DAG
處理台北市、新北市、桃園市、新竹市/竹北市、新竹縣地區
自動生成季度報告（堆疊橫條圖、箱型圖、地圖視覺化）並發送郵件
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import timedelta

from config.settings import default_args, CONTROL_VM_IP
from utils import log_to_gcs, calculate_target_year_season, get_pg_engine_with_retry, send_slack_success_notification, send_slack_failure_notification

from etls import extract_hsinchucity_data
from etls.loaders import load_email_recipients, load_email_sender
from pipelines.visualization import (
    create_stacked_bar_charts,
    create_summary_section,
    create_city_boxplots_combined,
    create_heat_maps
)
from pipelines.reporting import generate_html_report, send_report_email


# ==================== Airflow Tasks ====================

def load_and_process_data(**context):
    """從 PostgreSQL 載入並處理房地產資料"""
    log_to_gcs('INFO', "開始載入並處理資料...")
    
    engine = get_pg_engine_with_retry(**context)
    ti = context['task_instance']
    
    # 載入 email 設定
    try:
        recipients = load_email_recipients(engine)
        sender = load_email_sender(engine)
        
        # 驗證是否成功載入
        if not recipients:
            log_to_gcs('ERROR', "資料庫中無啟用的收件人")
            raise ValueError("未找到啟用的收件人")
        
        if not sender:
            log_to_gcs('ERROR', "資料庫中無寄件人")
            raise ValueError("未找到寄件人")
        
        ti.xcom_push(key='email_recipients', value=recipients)
        ti.xcom_push(key='email_sender', value=sender)
        
        log_to_gcs('INFO', f"Email 設定完成 - 寄件人: {sender}, 收件人數: {len(recipients)}")
        
    except Exception as e:
        log_to_gcs('ERROR', f"載入 Email 設定失敗: {str(e)}")
        raise
    
    year, season = calculate_target_year_season(context['execution_date'])
    log_to_gcs('INFO', f"查詢資料期間: 民國 {year} 年 Q{season}")
    
    # 使用 extractors 中的函數處理新竹市/竹北市合併
    df_used, df_new = extract_hsinchucity_data(engine, year, season)
    
    log_to_gcs('INFO', "資料處理完成")
    
    # 透過 XCom 傳遞資料
    ti.xcom_push(key='df_used', value=df_used.to_json(orient='split'))
    ti.xcom_push(key='df_new', value=df_new.to_json(orient='split'))
    ti.xcom_push(key='year', value=year)
    ti.xcom_push(key='season', value=season)
    
    return f"處理完成: 中古屋 {len(df_used)} 筆, 新成屋 {len(df_new)} 筆"


# ==================== DAG 定義 ====================

with DAG(
    'real_estate_quarterly_report',
    default_args=default_args,
    description='Generate and send quarterly real estate report with comprehensive charts and heat maps',
    schedule_interval='0 10 5 1,4,7,10 *',  # 每季資料釋出日 (1/5, 4/5, 7/5, 10/5) 早上 10 點執行
    catchup=False,
    tags=['production', 'report', 'email'],
) as dag:
    
    # Task 1: 載入並處理資料
    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_and_process_data,
        execution_timeout=timedelta(minutes=10),
    )
    
    # Task 2: 生成統計摘要區塊
    create_summary = PythonOperator(
        task_id='create_summary',
        python_callable=create_summary_section,
        execution_timeout=timedelta(minutes=10),
    )
    
    # Task 3: 生成堆疊橫條圖
    create_stacked_charts = PythonOperator(
        task_id='create_stacked_charts',
        python_callable=create_stacked_bar_charts,
        execution_timeout=timedelta(minutes=15),
    )
    
    # Task 4: 生成箱型圖
    create_boxplots = PythonOperator(
        task_id='create_boxplots',
        python_callable=create_city_boxplots_combined,
        execution_timeout=timedelta(minutes=15),
    )
    
    # Task 5: 生成熱度地圖
    create_heat_maps_task = PythonOperator(
        task_id='create_heat_maps',
        python_callable=create_heat_maps,
        execution_timeout=timedelta(minutes=30),
    )
    
    # Task 6: 生成 Email HTML 內容
    generate_email = PythonOperator(
        task_id='generate_email',
        python_callable=generate_html_report,
        execution_timeout=timedelta(minutes=5),
    )
    
    # Task 7: 發送郵件
    send_email = PythonOperator(
        task_id='send_email',
        python_callable=send_report_email,
        execution_timeout=timedelta(minutes=10),
    )
    
    # Task 8: 成功通知
    notify_success = PythonOperator(
    task_id='notify_success',
    python_callable=send_slack_success_notification,
    trigger_rule='all_success',
    )
    
    # Task 9: 失敗通知
    notify_failure = PythonOperator(
    task_id='notify_failure',
    python_callable=send_slack_failure_notification,
    trigger_rule='one_failed',
    )
    
    # DAG 的依賴關係
    load_data >> create_summary >> create_stacked_charts >> create_boxplots >> create_heat_maps_task >> generate_email >> send_email >> [notify_success, notify_failure]