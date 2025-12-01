"""
Airflow DAGs for Real Estate Data Pipeline

這個套件包含三個主要的 DAG：
1. real_estate_etl.py - 從政府平台擷取實價登錄資料並載入資料庫
2. real_estate_geocoding.py - 將地址轉換為經緯度座標
3. real_estate_report.py - 生成並發送季度報告

DAG 執行順序：
- ETL (每季 1/5, 4/5, 7/5, 10/5 凌晨 2:00)
- Geocoding (每季 1/5, 4/5, 7/5, 10/5 凌晨 4:00)
- Report (每季 1/5, 4/5, 7/5, 10/5 早上 9:00)
"""

# 確保 Airflow 能夠發現這個套件中的 DAG
# Airflow 會自動掃描 dags 資料夾中的 .py 檔案
# 這個 __init__.py 檔案可以保持空白，或者用於套件級別的初始化

__version__ = '1.0.0'
__author__ = 'Real Estate Data Team'

# DAG 資訊
DAGS = {
    'real_estate_quarterly_etl': {
        'description': 'Extract real estate data from government platform',
        'schedule': '0 2 5 1,4,7,10 *',
        'tags': ['production', 'etl', 'real-estate']
    },
    'real_estate_geocoding': {
        'description': 'Convert addresses to coordinates using HERE API',
        'schedule': '0 4 5 1,4,7,10 *',
        'tags': ['production', 'geocoding', 'real-estate']
    },
    'real_estate_quarterly_report': {
        'description': 'Generate and send quarterly real estate report',
        'schedule': '0 9 5 1,4,7,10 *',
        'tags': ['production', 'report', 'email']
    }
}