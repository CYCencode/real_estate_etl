#etls.loaders.py
"""資料載入模組"""

import pandas as pd
from sqlalchemy import text, inspect
import traceback

from config.settings import CITY_NAME_MAPPING
from utils.logging import log_to_gcs


def load_real_estate_data(engine, property_type, year, season):
    """
    查詢房地產資料的共用函數
    
    Args:
        engine: SQLAlchemy engine
        property_type: str, "used" 或 "presale"
        year: int, 民國年份
        season: int, 季度 (1-4)
    
    Returns:
        list: DataFrame 列表，每個縣市一個 DataFrame
    """
    property_type_name = "中古屋" if property_type == "used" else "新成屋"
    log_to_gcs('INFO', f"[查詢 {property_type_name}] 民國 {year} 年 第 {season} 季")
    
    df_list = []
    
    # 根據交易類型決定是否包含 build_year 欄位
    build_year_field = "build_year," if property_type == "used" else ""

    for city_name_english, city_name_chinese in CITY_NAME_MAPPING.items():
        table_name = f'real_estate_{property_type}_{city_name_english}'
        
        try:
            query = text(f"""
                SELECT 
                    land_total_sqm,
                    building_total_sqm,
                    room_count,
                    use_zone,
                    zip_zone,
                    unit_price_per_sqm,
                    total_price,
                    address,
                    locate_floor,
                    total_floor,
                    building_type,
                    transaction_year,
                    transaction_season,
                    {build_year_field}
                    latitude,
                    longitude
                FROM {table_name}
                WHERE transaction_year = :year
                  AND transaction_season = :season
                  AND latitude IS NOT NULL
                  AND longitude IS NOT NULL
            """)
            
            params = {'year': year, 'season': season}
            df_temp = pd.read_sql(query, engine, params=params)
            
            if len(df_temp) > 0:
                df_temp['city'] = city_name_chinese
                df_list.append(df_temp)
                
                log_to_gcs('INFO', f"載入 {city_name_chinese} {property_type_name}資料: {len(df_temp)} 筆")
                if 'building_type' in df_temp.columns:
                    building_types = df_temp['building_type'].value_counts().to_dict()
                    log_to_gcs('INFO', f"建物類型: {building_types}")
            else:
                log_to_gcs('INFO', f"{city_name_chinese} {property_type_name}無資料")
                
        except Exception as e:
            log_to_gcs('WARNING', f"查詢 {table_name} 失敗: {str(e)}")
    
    return df_list

def load_email_recipients(engine):
    """
    載入已啟用的收件人清單
    
    Args:
        engine: SQLAlchemy engine
    
    Returns:
        list: 收件人 email 列表
    """
    try:
        query = text("""
            SELECT email
            FROM email_recipients 
            WHERE is_enabled = 1
            ORDER BY created_time ASC
        """)
        
        df = pd.read_sql(query, engine)
        
        if len(df) == 0:
            log_to_gcs('WARNING', "未找到任何已啟用的收件人")
            return []
        
        recipients = df['email'].tolist()
        log_to_gcs('INFO', f"載入 {len(recipients)} 位收件人")
        
        return recipients
        
    except Exception as e:
        log_to_gcs('ERROR', f"載入收件人清單失敗: {str(e)}", details=traceback.format_exc())
        raise

def load_email_sender(engine):
    """
    載入寄件人信箱（優先使用管理員帳號）
    
    Args:
        engine: SQLAlchemy engine
    
    Returns:
        str: 寄件人 email
    """
    try:
        # 優先查詢管理員帳號
        query = text("""
            SELECT email
            FROM email_sender 
            WHERE is_admin = 1
            ORDER BY created_time ASC
            LIMIT 1
        """)
        
        df = pd.read_sql(query, engine)
        
        # 如果沒有管理員，取第一個帳號
        if len(df) == 0:
            log_to_gcs('WARNING', "未找到管理員帳號，查詢一般寄件人")
            query = text("""
                SELECT email
                FROM email_sender 
                ORDER BY created_time ASC
                LIMIT 1
            """)
            df = pd.read_sql(query, engine)
        
        if len(df) == 0:
            log_to_gcs('ERROR', "未找到任何寄件人帳號")
            raise ValueError("資料庫中未設定寄件人")
        
        sender_email = df.iloc[0]['email']
        log_to_gcs('INFO', f"使用寄件人: {sender_email}")
        
        return sender_email
        
    except Exception as e:
        log_to_gcs('ERROR', f"載入寄件人失敗: {str(e)}", details=traceback.format_exc())
        raise