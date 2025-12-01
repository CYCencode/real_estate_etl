#pipelines/reporting.py
"""報告生成與發送模組"""

import os
import traceback
from datetime import datetime
import pandas as pd
from sqlalchemy import text
import smtplib
import requests
import base64
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from io import StringIO
from airflow.exceptions import AirflowException

from config import CITY_NAME_MAPPING, SMTP_HOST, SMTP_PORT
from utils import log_to_gcs, get_quarter_display_info, get_pg_engine_with_retry
from etls import extract_hsinchucity_data, load_email_recipients, load_email_sender
from pipelines.visualization import (
    create_stacked_bar_charts_standalone,
    create_summary_section_standalone,
    create_city_boxplots_combined_standalone,
    create_heat_maps_standalone
)
import pytz

# 定義台北時區
taipei_tz = pytz.timezone('Asia/Taipei')

def generate_report_content(year: int, season: int, engine) -> dict:
    """
    生成報告內容（不依賴 Airflow context）
    
    Args:
        year: 民國年
        season: 季度
        engine: SQLAlchemy engine
    
    Returns:
        {
            'html_content': str,
            'all_image_urls': list,
            'year': int,
            'season': int
        }
    """
    log_to_gcs('INFO', f"生成報告內容: {year}S{season}")
    
    # 1. 載入資料
    df_used, df_new = extract_hsinchucity_data(engine, year, season)
    
    if df_used.empty and df_new.empty:
        raise ValueError(f"查無 {year}S{season} 的資料")
    
    # 2. 生成所有圖表（使用 standalone 版本）
    stacked_charts = create_stacked_bar_charts_standalone(df_used, df_new, year, season)
    summary_data = create_summary_section_standalone(df_used, df_new, year, season)
    boxplot_urls = create_city_boxplots_combined_standalone(df_used, df_new, year, season)
    heat_map_urls = create_heat_maps_standalone(df_used, df_new, year, season)
    
    # 3. 組裝 HTML 各區塊
    start_date, end_date = get_quarter_display_info(year, season)
    
    # 構建堆疊橫條圖區塊
    stacked_charts_html = ""
    if 'total_price' in stacked_charts:
        stacked_charts_html += f'<h3>總價分布</h3><img src="{stacked_charts["total_price"]}" alt="總價分布" style="max-width: 100%; height: auto; margin: 20px 0;">'
    if 'building_area' in stacked_charts:
        stacked_charts_html += f'<h3>建坪分布</h3><img src="{stacked_charts["building_area"]}" alt="建坪分布" style="max-width: 100%; height: auto; margin: 20px 0;">'
    if 'building_type' in stacked_charts:
        stacked_charts_html += f'<h3>建物類型分布</h3><img src="{stacked_charts["building_type"]}" alt="建物類型分布" style="max-width: 100%; height: auto; margin: 20px 0;">'
    if 'building_age' in stacked_charts:
        stacked_charts_html += f'<h3>屋齡分布(僅中古屋)</h3><img src="{stacked_charts["building_age"]}" alt="屋齡分布" style="max-width: 100%; height: auto; margin: 20px 0;">'
    
    # 構建箱型圖區塊
    boxplot_html = ""
    for item in boxplot_urls:
        boxplot_html += f'<h3>{item["city"]}</h3>'
        boxplot_html += f'<img src="{item["url"]}" alt="{item["city"]}箱型圖" style="max-width: 100%; height: auto; margin: 20px 0;">'
    
    # 構建區域熱度地圖區塊
    heat_map_html = ""
    for item in heat_map_urls:
        heat_map_html += f'<h3>{item["city"]} - {item["type"]}</h3>'
        heat_map_html += f'<img src="{item["url"]}" alt="{item["city"]} {item["type"]}熱度圖" style="max-width: 100%; height: auto; margin: 20px 0;">'
    
    # 從 summary_data 取得資料
    transaction_count_url = summary_data['transaction_count_url']
    table_html = summary_data['table_html']
    
    # 4. 組裝完整 HTML
    html_content = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<style>
body {{ font-family: Arial, 'Microsoft JhengHei', sans-serif; line-height: 1.6; color: #000000 !important; }}
.container {{ max-width: 1200px; margin: 0 auto; padding: 20px; }}
h1, h2, h3, p, td, th, div {{ color: #000000 !important; }}
h1 {{ color: #1F2937 !important; border-bottom: 3px solid #3B82F6; padding-bottom: 15px; }}
h2 {{ color: #1F2937 !important; border-bottom: 2px solid #3B82F6; padding-bottom: 10px; margin-top: 30px; }}
h3 {{ color: #1F2937 !important; margin-top: 20px; }}
.section {{ margin: 30px 0; }}
img {{ max-width: 100%; height: auto; display: block; margin: 20px 0; }}
</style>
</head>
<body>
<div class="container">
<h1>台灣北部大都會區域房地產季報</h1>
<p>您好，</p>
<p>民國 {year} 年 Q{season} ({start_date} ~ {end_date}) 台灣北部大都會區域(台北市、新北市、桃園市、新竹市/竹北市、新竹縣)的中古屋/新成屋成交情形如下所示：</p>

<div class="section">
<h2>1. 總價、建坪、建物類型、屋齡成交件數分布</h2>
{stacked_charts_html}
</div>

<div class="section">
<h2>2. 各縣市區域房價分布與成交量(箱型圖)</h2>
{boxplot_html}
</div>

<div class="section">
<h2>3. 各縣市區域熱度地圖</h2>
<p>以下地圖顯示各縣市的房地產成交分布，點的顏色代表總價區間(深色=低價，淺色=高價)，點的大小代表坪數大小。</p>
{heat_map_html}
</div>

<div class="section">
<h2>4. 各縣市統計摘要</h2>
<img src="{transaction_count_url}" alt="成交件數分布" style="max-width: 100%; height: auto; margin: 20px 0;">
{table_html}
</div>

<hr style="margin: 40px 0; border: none; border-top: 1px solid #E5E7EB;">
<p style="font-size: 12px; color: #6B7280 !important;">
<strong style="color: #000000 !important;">資料提供：</strong>內政部不動產實價登錄<br>
<strong style="color: #000000 !important;">報告生成時間：</strong>{datetime.now(taipei_tz).strftime('%Y-%m-%d %H:%M:%S')}<br>
<strong style="color: #000000 !important;">ETL 觸發時機：</strong>每季 (1/5, 4/5, 7/5, 10/5)<br>
</p>
</div>
</body>
</html>"""
    
    # 5. 收集所有圖片 URL 用於 Email 附件
    all_image_urls = []
    
    for key, url in stacked_charts.items():
        all_image_urls.append({'name': key, 'url': url})
    
    all_image_urls.append({'name': 'transaction_count', 'url': transaction_count_url})
    
    for item in boxplot_urls:
        all_image_urls.append({'name': f'boxplot_{item["city"]}', 'url': item['url']})
    
    for item in heat_map_urls:
        all_image_urls.append({'name': f'heatmap_{item["city"]}_{item["type"]}', 'url': item['url']})
    
    log_to_gcs('INFO', "Email 內容生成完成")
    
    return {
        'html_content': html_content,
        'all_image_urls': all_image_urls,
        'year': year,
        'season': season
    }


def send_email_to_recipients(html_content: str, all_image_urls: list, 
                             recipients: list, sender: str, 
                             year: int, season: int) -> dict:
    """
    發送郵件給指定收件人（不依賴 Airflow context）
    
    Args:
        html_content: HTML 郵件內容
        all_image_urls: 圖片 URL 列表
        recipients: 收件人 email 列表
        sender: 寄件人 email
        year: 民國年
        season: 季度
    
    Returns:
        {
            'success_count': int,
            'failed_recipients': list
        }
    """
    smtp_host = SMTP_HOST
    smtp_port = SMTP_PORT
    gmail_password = os.environ.get('GMAIL_APP_PASSWORD', '')
    
    if not gmail_password:
        raise ValueError("請設定 GMAIL_APP_PASSWORD 環境變數")
    
    if not recipients:
        raise ValueError("未設定收件人")
    if not sender:
        raise ValueError("未設定寄件人")
    
    log_to_gcs('INFO', f"寄件人: {sender}")
    log_to_gcs('INFO', f"準備寄送給 {len(recipients)} 位收件人")
    
    def get_image_data(url):
        """從 URL 或 base64 data URL 獲取圖片數據"""
        if url.startswith('data:image'):
            header, encoded = url.split(',', 1)
            return base64.b64decode(encoded)
        else:
            try:
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                return response.content
            except Exception as e:
                log_to_gcs('ERROR', f"Failed to download image from {url}: {str(e)}")
                raise
    
    try:
        image_data_list = []
        total_size = 0

        for item in all_image_urls:
            data = get_image_data(item['url'])
            size_kb = len(data) / 1024
            total_size += size_kb

            image_data_list.append({
                'name': item['name'],
                'data': data
            })
        
            log_to_gcs('INFO', f"圖片 {item['name']}: {size_kb:.1f}KB")
        
        log_to_gcs('INFO', f"已處理 {len(image_data_list)} 張圖片，總大小: {total_size/1024:.2f}MB")
        
        if total_size > 20 * 1024:
            log_to_gcs('WARNING', f"郵件大小 {total_size/1024:.2f}MB 接近 Gmail 限制")

    except Exception as e:
        log_to_gcs('ERROR', f"Failed to process charts: {str(e)}")
        raise
    
    # 替換 HTML 中的圖片 URL 為 CID
    for idx, item in enumerate(all_image_urls):
        html_content = html_content.replace(item['url'], f'cid:image_{idx}')
    
    # 迴圈寄送
    success_count = 0
    failed_recipients = []
    
    try:
        log_to_gcs('INFO', f"Connecting to {SMTP_HOST}:{SMTP_PORT}...")
        server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30)
        server.starttls()
        server.login(sender, gmail_password)
        log_to_gcs('INFO', "SMTP 連線成功")
        
        for recipient in recipients:
            try:
                msg = MIMEMultipart('related')
                msg['Subject'] = f"[房地產季報]民國 {year} 年 Q{season} 台灣北部大都會區域成交分析"
                msg['From'] = sender
                msg['To'] = recipient
                
                msg.attach(MIMEText(html_content, 'html', 'utf-8'))
                
                for idx, item in enumerate(image_data_list):
                    img = MIMEImage(item['data'])
                    img.add_header('Content-ID', f'<image_{idx}>')
                    img.add_header('Content-Disposition', 'inline', filename=f'{item["name"]}.png')
                    msg.attach(img)
                
                server.send_message(msg)
                success_count += 1
                log_to_gcs('INFO', f"✓ 成功發送給: {recipient} ({success_count}/{len(recipients)})")
                
            except Exception as e:
                failed_recipients.append({'email': recipient, 'error': str(e)})
                log_to_gcs('ERROR', f"✗ 發送失敗: {recipient} - {str(e)}")
        
        server.quit()
        log_to_gcs('INFO', "SMTP 連線已關閉")
        
    except Exception as e:
        log_to_gcs('ERROR', f"SMTP 連線失敗: {str(e)}", details=traceback.format_exc())
        raise Exception(f"SMTP connection failed: {str(e)}")
    
    summary = f"郵件發送完成: 成功 {success_count}/{len(recipients)}"
    
    if failed_recipients:
        summary += f", 失敗 {len(failed_recipients)} 位"
        log_to_gcs('WARNING', f"失敗名單: {failed_recipients}")
    
    log_to_gcs('INFO', summary)
    
    if success_count == 0:
        raise Exception("所有收件人發送失敗")
    
    return {
        'success_count': success_count,
        'failed_recipients': failed_recipients
    }


# ==================== Airflow DAG 使用的函數 ====================

def generate_html_report(**context):
    """生成 Email HTML 內容（Airflow DAG 使用）"""
    log_to_gcs('INFO', "生成 Email 內容...")
    
    ti = context['task_instance']
    
    year = ti.xcom_pull(task_ids='load_data', key='year')
    season = ti.xcom_pull(task_ids='load_data', key='season')
    
    # 取得收件人資訊
    recipients = ti.xcom_pull(task_ids='load_data', key='email_recipients')
    recipients_count = len(recipients) if recipients else 0
    log_to_gcs('INFO', f"報告將寄送給 {recipients_count} 位收件人")
    
    # 使用新的模組化函數
    engine = get_pg_engine_with_retry(**context)
    report_data = generate_report_content(year, season, engine)
    
    # 推送到 XCom（保持原有 DAG 運作）
    ti.xcom_push(key='email_html', value=report_data['html_content'])
    ti.xcom_push(key='all_image_urls', value=report_data['all_image_urls'])
    
    log_to_gcs('INFO', "Email 內容生成完成")
    return report_data['html_content']


def send_report_email(**context):
    """發送郵件（Airflow DAG 使用）"""
    ti = context['task_instance']
    
    # 取得收件 & 寄件人資訊（從資料庫）
    recipients = ti.xcom_pull(task_ids='load_data', key='email_recipients')
    sender = ti.xcom_pull(task_ids='load_data', key='email_sender')
    
    html_content = ti.xcom_pull(task_ids='generate_email', key='email_html')
    all_image_urls = ti.xcom_pull(task_ids='generate_email', key='all_image_urls')
    
    year = ti.xcom_pull(task_ids='load_data', key='year')
    season = ti.xcom_pull(task_ids='load_data', key='season')
    
    # 使用新的模組化函數
    result = send_email_to_recipients(
        html_content=html_content,
        all_image_urls=all_image_urls,
        recipients=recipients,
        sender=sender,
        year=year,
        season=season
    )
    
    # 將失敗資訊推送到 XCom
    if result['failed_recipients']:
        ti.xcom_push(key='failed_recipients', value=result['failed_recipients'])
    
    return f"成功發送 {result['success_count']}/{len(recipients)} 封郵件"
