#pipelines/google_geocoding.py
"""Google 地址轉經緯度處理模組"""

import time
import traceback
import pandas as pd
import googlemaps
from sqlalchemy import text, inspect

from utils.logging import log_to_gcs


def geocode_address_google(address, gmaps_client):
    """
    使用 Google Geocoding API 將地址轉換為座標
    
    Args:
        address: str, 地址字串
        gmaps_client: googlemaps.Client 實例
    
    Returns:
        dict: 包含 latitude, longitude, formatted_address, status
    """
    try:
        # 呼叫 Geocoding API
        geocode_result = gmaps_client.geocode(address, language='zh-TW')
        
        if geocode_result:
            # 取得第一筆結果
            result = geocode_result[0]
            location = result['geometry']['location']
            
            return {
                'latitude': location['lat'],
                'longitude': location['lng'],
                'formatted_address': result.get('formatted_address', ''),
                'location_type': result['geometry'].get('location_type', ''),
                'status': 'SUCCESS'
            }
        else:
            return {
                'latitude': None,
                'longitude': None,
                'formatted_address': None,
                'location_type': None,
                'status': 'NO_RESULTS'
            }
    
    except googlemaps.exceptions.ApiError as e:
        return {
            'latitude': None,
            'longitude': None,
            'formatted_address': None,
            'location_type': None,
            'status': f'API_ERROR: {str(e)}'
        }
    except Exception as e:
        return {
            'latitude': None,
            'longitude': None,
            'formatted_address': None,
            'location_type': None,
            'status': f'ERROR: {str(e)}'
        }


def check_and_add_columns_google(engine, table_name):
    """
    檢查並新增 latitude 和 longitude 欄位（如果不存在）
    
    Args:
        engine: SQLAlchemy engine
        table_name: str, 資料表名稱
    
    Returns:
        bool: True if columns were added or already exist
    """
    try:
        inspector = inspect(engine)
        columns = [col['name'] for col in inspector.get_columns(table_name)]
        
        columns_to_add = []
        if 'latitude' not in columns:
            columns_to_add.append('latitude')
        if 'longitude' not in columns:
            columns_to_add.append('longitude')
        
        if columns_to_add:
            with engine.begin() as conn:
                for col in columns_to_add:
                    alter_query = text(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {col} DOUBLE PRECISION")
                    conn.execute(alter_query)
                    log_to_gcs('INFO', f"新增欄位 {col} 至 {table_name}")
        else:
            log_to_gcs('INFO', f"{table_name} 已包含 latitude 和 longitude 欄位")
        
        return True
        
    except Exception as e:
        log_to_gcs('ERROR', f"檢查/新增欄位失敗: {table_name}", details={
            "error": str(e),
            "traceback": traceback.format_exc()
        })
        return False


def process_table_google_geocoding(table_name, api_key, engine, batch_size=100, request_delay=0.1):
    """
    使用 Google Geocoding API 將指定資料表的地址轉經緯度並回存
    
    Args:
        table_name: str, 資料表名稱
        api_key: str, Google Maps API Key
        engine: SQLAlchemy engine
        batch_size: int, 每批次處理筆數 (建議 100)
        request_delay: float, 每個請求之間的延遲（秒，建議 0.1）
    
    Returns:
        dict: 處理結果統計
    """
    log_to_gcs('INFO', f"開始處理資料表: {table_name}")
    
    try:
        # 初始化 Google Maps client
        gmaps = googlemaps.Client(key=api_key)
        log_to_gcs('INFO', "Google Maps Client 初始化成功")
        
        # 檢查並新增欄位
        if not check_and_add_columns_google(engine, table_name):
            return {
                'table': table_name,
                'status': 'failed',
                'error': 'Failed to add columns'
            }
        
        # 讀取尚未轉換的資料（latitude 為 NULL 的記錄）
        query = text(f"""
            SELECT id, address 
            FROM {table_name} 
            WHERE latitude IS NULL 
            AND address IS NOT NULL 
            AND address != ''
            ORDER BY id
        """)
        
        df = pd.read_sql(query, engine)
        
        if df.empty:
            log_to_gcs('INFO', f"{table_name} 沒有需要處理的地址", details={
                "total_records": 0
            })
            return {
                'table': table_name,
                'status': 'skipped',
                'total': 0,
                'success': 0,
                'failed': 0
            }
        
        total_addresses = len(df)
        log_to_gcs('INFO', f"{table_name} 共有 {total_addresses} 筆地址需要轉換")
        
        # 批次處理地址
        results = []
        success_count = 0
        failed_count = 0
        
        for idx, row in df.iterrows():
            # 顯示進度
            if (idx + 1) % batch_size == 0:
                log_to_gcs('INFO', f"{table_name} 處理進度: {idx + 1}/{total_addresses}")
            
            address = row['address']
            record_id = row['id']
            
            result = geocode_address_google(address, gmaps)
            result['id'] = record_id
            result['address'] = address
            results.append(result)
            
            if result['status'] == 'SUCCESS':
                success_count += 1
            else:
                failed_count += 1
                # 記錄前 10 筆失敗案例以便除錯
                if failed_count <= 10:
                    log_to_gcs('WARNING', f"Geocoding 失敗", details={
                        'address': address,
                        'status': result['status'],
                        'id': record_id
                    })
            
            # 避免超過 API 使用限制，每個請求後都延遲
            time.sleep(request_delay)
        
        log_to_gcs('INFO', f"{table_name} 地址轉換完成", details={
            "total": total_addresses,
            "success": success_count,
            "failed": failed_count,
            "success_rate": f"{success_count/total_addresses*100:.1f}%"
        })
        
        # 將結果寫回資料庫
        df_results = pd.DataFrame(results)
        
        # 只更新成功轉換的記錄
        df_success = df_results[df_results['status'] == 'SUCCESS'].copy()
        
        if not df_success.empty:
            update_count = 0
            # 自動管理 transaction
            with engine.begin() as conn:
                for _, row in df_success.iterrows():
                    update_query = text(f"""
                        UPDATE {table_name}
                        SET latitude = :lat, longitude = :lng
                        WHERE id = :id
                    """)
                    conn.execute(update_query, {
                        'lat': row['latitude'],
                        'lng': row['longitude'],
                        'id': row['id']
                    })
                    update_count += 1
            
            log_to_gcs('INFO', f"{table_name} 成功更新 {update_count} 筆記錄至資料庫")
        
        return {
            'table': table_name,
            'status': 'completed',
            'total': total_addresses,
            'success': success_count,
            'failed': failed_count,
            'updated': len(df_success)
        }
        
    except Exception as e:
        log_to_gcs('ERROR', f"{table_name} 處理失敗", details={
            "error": str(e),
            "traceback": traceback.format_exc()
        })
        return {
            'table': table_name,
            'status': 'error',
            'error': str(e)
        }
