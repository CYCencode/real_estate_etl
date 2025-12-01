#utils/helpers.py
"""輔助函數模組"""

from datetime import datetime
from google.cloud import storage
from io import BytesIO
import base64
from PIL import Image

from config import GCS_BUCKET_NAME, CONTROL_VM_IP 
from utils import log_to_gcs

import os
import requests
from airflow.exceptions import AirflowException

def calculate_target_year_season(execution_date=None):
    """
    根據執行時間計算要查詢的年份和季度
    資料釋出時間: 1/5=S4, 4/5=S1, 7/5=S2, 10/5=S3
    
    Args:
        execution_date: datetime 物件（選填）
                       - 提供時：用於 Airflow DAG（使用 execution_date）
                       - 不提供時：用於 API（使用當下時間）
    
    Returns:
        (year, season): 民國年, 季度
    
    Examples:
        >>> # API 使用（使用當下時間）
        >>> year, season = calculate_target_year_season()
        
        >>> # Airflow DAG 使用（使用 execution_date）
        >>> year, season = calculate_target_year_season(context['execution_date'])
    """
    # 如果沒有提供 execution_date，使用當下時間（API 使用情境）
    if execution_date is None:
        execution_date = datetime.now()
    
    release_schedule = [
        (1, 5, 4, -1),   # 1/5 釋出前一年 S4
        (4, 5, 1, 0),    # 4/5 釋出當年 S1
        (7, 5, 2, 0),    # 7/5 釋出當年 S2
        (10, 5, 3, 0),   # 10/5 釋出當年 S3
    ]
    
    year = execution_date.year
    month = execution_date.month
    day = execution_date.day
    
    target_year = None
    target_season = None
    
    for release_month, release_day, season, year_offset in release_schedule:
        if (month > release_month) or (month == release_month and day >= release_day):
            target_season = season
            target_year = year + year_offset
    
    if target_season is None:
        target_year = year - 1
        target_season = 4
    
    minguo_year = target_year - 1911
    
    log_msg = f"執行日期: {execution_date.strftime('%Y/%m/%d')}, 目標資料: {minguo_year}S{target_season} (西元 {target_year} 年第 {target_season} 季)"
    log_to_gcs('INFO', log_msg, details={
        "execution_date": execution_date.isoformat(),
        "target_year_minguo": minguo_year,
        "target_year_ad": target_year,
        "target_season": target_season
    })
    
    return minguo_year, target_season


def get_quarter_display_info(year, season):
    """根據年份和季度生成顯示用的日期範圍"""
    quarter_dates = {
        1: ('01-01', '03-31'),
        2: ('04-01', '06-30'),
        3: ('07-01', '09-30'),
        4: ('10-01', '12-31'),
    }
    
    start, end = quarter_dates[season]
    return f"{year}/{start}", f"{year}/{end}"


def upload_chart_to_gcs(buffer, chart_name, year, season, compress=False, quality=None):
    """將圖片上傳到 GCS，返回 base64 data URL"""
    try:
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET_NAME)
        
        # 使用 year/season 資料夾結構
        folder_path = get_report_charts_folder(year, season)
        blob_path = f"{folder_path}{chart_name}.png"
        
        # 檢查是否錯誤啟用壓縮
        if compress:
            log_to_gcs('ERROR', f"❌ 壓縮不應啟用！chart={chart_name}")
            raise ValueError("不應該壓縮圖片！請檢查程式碼")
        
        # 上傳原始 PNG（不壓縮）
        blob = bucket.blob(blob_path)
        buffer.seek(0)
        blob.upload_from_file(buffer, content_type='image/png')
        
        log_to_gcs('INFO', f"✅ 圖表上傳成功: gs://{GCS_BUCKET_NAME}/{blob_path}")
        
        # 返回 base64 data URL
        buffer.seek(0)
        base64_img = base64.b64encode(buffer.read()).decode()
        return f"data:image/png;base64,{base64_img}"
        
    except Exception as e:
        log_to_gcs('ERROR', f"❌ 上傳失敗: {str(e)}")
        # 發生錯誤時仍返回 base64（避免完全失敗）
        buffer.seek(0)
        return f"data:image/png;base64,{base64.b64encode(buffer.read()).decode()}"
def get_report_charts_folder(year: int, season: int) -> str:
    """
    取得報告圖表的 GCS 資料夾路徑
    
    Args:
        year: 民國年
        season: 季度 (1-4)
    
    Returns:
        GCS 資料夾路徑，例如: "reports/charts/113s4/"
    """
    return f"reports/charts/{year}s{season}/"


def check_charts_exist(year: int, season: int) -> dict:
    """
    檢查指定期間的圖表是否已存在於 GCS
    
    Args:
        year: 民國年
        season: 季度 (1-4)
    
    Returns:
        {
            'exists': bool,  # 是否存在完整的圖表
            'folder': str,   # GCS 資料夾路徑
            'charts': dict,  # 已存在的圖表 {chart_name: blob_path}
            'missing': list  # 缺少的圖表名稱
        }
    """
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    
    folder_path = get_report_charts_folder(year, season)
    
    # 定義必要的圖表清單
    required_charts = [
        'total_price_stacked',
        'building_area_stacked',
        'building_type_stacked',
        'building_age_stacked',
        'transaction_count_stacked',
        # 箱型圖
        'boxplot_Taipei_combined',
        'boxplot_NewTaipei_combined',
        'boxplot_Taoyuan_combined',
        'boxplot_HsinchuCity_combined',
        'boxplot_HsinchuCounty_combined',
        # 熱度地圖 (中古屋)
        'heatmap_Taipei_used',
        'heatmap_NewTaipei_used',
        'heatmap_Taoyuan_used',
        'heatmap_HsinchuCity_used',
        'heatmap_HsinchuCounty_used',
        # 熱度地圖 (新成屋)
        'heatmap_Taipei_presale',
        'heatmap_NewTaipei_presale',
        'heatmap_Taoyuan_presale',
        'heatmap_HsinchuCity_presale',
        'heatmap_HsinchuCounty_presale',
    ]
    
    # 列出資料夾中的所有檔案
    blobs = list(bucket.list_blobs(prefix=folder_path))
    
    if not blobs:
        log_to_gcs('INFO', f"圖表資料夾不存在: {folder_path}")
        return {
            'exists': False,
            'folder': folder_path,
            'charts': {},
            'missing': required_charts
        }
    
    # 建立已存在的圖表映射 {chart_name: blob_path}
    existing_charts = {}
    for blob in blobs:
        # 從 blob 名稱提取圖表名稱（移除副檔名）
        # 例如: "reports/charts/113s4/total_price_stacked.png" -> "total_price_stacked"
        file_name = blob.name.replace(folder_path, '').replace('.png', '').replace('.jpg', '').replace('.jpeg', '')
        if file_name:
            existing_charts[file_name] = blob.name
    
    # 找出缺少的圖表
    missing_charts = [chart for chart in required_charts if chart not in existing_charts]
    
    all_exist = len(missing_charts) == 0
    
    if all_exist:
        log_to_gcs('INFO', f"圖表已完整存在於 {folder_path}，共 {len(existing_charts)} 張")
    else:
        log_to_gcs('INFO', f"圖表不完整: 已有 {len(existing_charts)} 張，缺少 {len(missing_charts)} 張")
        log_to_gcs('INFO', f"缺少的圖表: {missing_charts}")
    
    return {
        'exists': all_exist,
        'folder': folder_path,
        'charts': existing_charts,
        'missing': missing_charts
    }


def get_chart_url_from_gcs(year: int, season: int, chart_name: str) -> str:
    """
    從 GCS 取得圖表的 base64 data URL
    
    Args:
        year: 民國年
        season: 季度
        chart_name: 圖表名稱（不含副檔名）
    
    Returns:
        base64 data URL
    """
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    
    folder_path = get_report_charts_folder(year, season)
    
    # 嘗試多種副檔名
    for ext in ['.png', '.jpg', '.jpeg']:
        blob_path = f"{folder_path}{chart_name}{ext}"
        blob = bucket.blob(blob_path)
        
        if blob.exists():
            # 下載圖片資料
            image_data = blob.download_as_bytes()
            base64_data = base64.b64encode(image_data).decode()
            
            # 判斷圖片類型
            img_type = 'jpeg' if ext in ['.jpg', '.jpeg'] else 'png'
            
            log_to_gcs('INFO', f"從 GCS 載入圖表: {blob_path}")
            return f"data:image/{img_type};base64,{base64_data}"
    
    # 找不到圖表
    log_to_gcs('ERROR', f"在 GCS 中找不到圖表: {folder_path}{chart_name}")
    raise FileNotFoundError(f"圖表不存在: {chart_name}")


def load_existing_charts(year: int, season: int) -> dict:
    """
    載入已存在的所有圖表
    
    Args:
        year: 民國年
        season: 季度
    
    Returns:
        {
            'stacked_charts': dict,
            'transaction_count_url': str,
            'boxplot_urls': list,
            'heat_map_urls': list
        }
    """
    log_to_gcs('INFO', f"開始載入 {year}S{season} 的現有圖表...")
    
    # 檢查圖表是否存在
    check_result = check_charts_exist(year, season)
    
    if not check_result['exists']:
        raise FileNotFoundError(f"圖表不完整，缺少: {check_result['missing']}")
    
    # 載入堆疊橫條圖
    stacked_charts = {}
    for chart_type in ['total_price', 'building_area', 'building_type', 'building_age']:
        chart_name = f"{chart_type}_stacked"
        stacked_charts[chart_type] = get_chart_url_from_gcs(year, season, chart_name)
    
    # 載入成交件數圖
    transaction_count_url = get_chart_url_from_gcs(year, season, 'transaction_count_stacked')
    
    # 載入箱型圖
    boxplot_urls = []
    city_codes = ['Taipei', 'NewTaipei', 'Taoyuan', 'HsinchuCity', 'HsinchuCounty']
    city_names = ['台北市', '新北市', '桃園市', '新竹市/竹北市', '新竹縣']
    
    for code, name in zip(city_codes, city_names):
        chart_name = f"boxplot_{code}_combined"
        try:
            url = get_chart_url_from_gcs(year, season, chart_name)
            boxplot_urls.append({
                'city': name,
                'url': url
            })
        except FileNotFoundError:
            log_to_gcs('WARNING', f"找不到 {name} 的箱型圖")
    
    # 載入熱度地圖
    heat_map_urls = []
    for code, name in zip(city_codes, city_names):
        for map_type in ['used', 'presale']:
            chart_name = f"heatmap_{code}_{map_type}"
            try:
                url = get_chart_url_from_gcs(year, season, chart_name)
                heat_map_urls.append({
                    'city': name,
                    'type': '中古屋' if map_type == 'used' else '新成屋',
                    'url': url
                })
            except FileNotFoundError:
                log_to_gcs('WARNING', f"找不到 {name} {map_type} 的熱度地圖")
    
    log_to_gcs('INFO', f"成功載入所有圖表: 堆疊圖 {len(stacked_charts)} 張, 箱型圖 {len(boxplot_urls)} 張, 熱度圖 {len(heat_map_urls)} 張")
    
    return {
        'stacked_charts': stacked_charts,
        'transaction_count_url': transaction_count_url,
        'boxplot_urls': boxplot_urls,
        'heat_map_urls': heat_map_urls
    }

def send_slack_success_notification(**context):
    """發送成功通知到 Slack"""
    webhook_url = os.getenv('SLACK_WEBHOOK_URL')
    if not webhook_url:
        log_to_gcs('WARNING', "SLACK_WEBHOOK_URL not set, skipping notification")
        return
    
    ti = context['ti']
    dag_id = context['dag'].dag_id
    
    # 根據不同的 DAG 使用不同的訊息
    if dag_id == 'real_estate_quarterly_etl':
        year = ti.xcom_pull(task_ids='run_etl', key='year')
        season = ti.xcom_pull(task_ids='run_etl', key='season')
        total_rows = ti.xcom_pull(task_ids='run_etl', key='total_rows')
        duration = ti.xcom_pull(task_ids='run_etl', key='duration')
        
        message = f"""Real Estate ETL Execution Report
━━━━━━━━━━━━━━━━━━━━━━━━
Status: ✅ SUCCESS
Data Period: {year}S{season}
Records Processed: {total_rows} rows
Execution Time: {duration} seconds
"""
    
    elif dag_id == 'real_estate_quarterly_report':
        year = ti.xcom_pull(task_ids='load_data', key='year')
        season = ti.xcom_pull(task_ids='load_data', key='season')
        
        # 取得發送結果
        success_count = ti.xcom_pull(task_ids='send_email', key='return_value')
        
        message = f"""Real Estate Report Email Sent
━━━━━━━━━━━━━━━━━━━━━━━━
Status: ✅ SUCCESS
Report Period: {year}S{season}
{success_count}
"""
    else:
        message = f"DAG {dag_id} completed successfully"
    
    # 發送通知
    try:
        response = requests.post(
            webhook_url,
            json={"text": message},
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        
        if response.status_code == 200:
            log_to_gcs('INFO', 'Slack notification sent successfully')
        else:
            log_to_gcs('ERROR', f"Slack notification failed: {response.status_code}")
    except Exception as e:
        log_to_gcs('ERROR', f'Failed to send Slack notification: {str(e)}')
def send_slack_failure_notification(**context):
    """發送失敗通知到 Slack"""
    webhook_url = os.getenv('SLACK_WEBHOOK_URL')
    if not webhook_url:
        return  # 如果沒有 webhook URL,靜默失敗
    
    ds = context['ds']
    execution_date = context['execution_date']
    
    message = f"""Real Estate ETL Execution Report
━━━━━━━━━━━━━━━━━━━━━━━━
Status: ❌ FAILED
Timestamp: {ds} {execution_date.strftime('%H:%M:%S')}

An error occurred during pipeline execution.

[View Logs] http://{CONTROL_VM_IP}:8080/dags/real_estate_quarterly_etl/grid
"""
    
    try:
        requests.post(webhook_url, json={"text": message}, timeout=10)
    except Exception as e:
        log_to_gcs('ERROR', f'Failed to send Slack notification: {str(e)}')
