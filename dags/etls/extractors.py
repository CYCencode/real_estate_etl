# etls/extractors
"""資料擷取模組"""

import requests
import pandas as pd
from io import StringIO
import numpy as np
import time
import traceback
from sqlalchemy import text, inspect

from config.settings import (
    AREA_CODE_NAME,
    TRADE_TYPE_MAPPING,
    COLUMNS_MAPPING,
    FILTER_FOR_TRANSFER_TYPE,
    FILTER_FOR_SPECIAL_DEAL,
    FILTER_FOR_BUILDING_TYPE,
    CITY_NAME_MAPPING
)
from utils.logging import log_to_gcs
from etls.transformers import convert_chinese_floor_to_number, parse_date_fields

def create_table_with_constraints(table_name, engine, is_used=False):
    """
    創建資料表並設定約束條件
    - 自動遞增的 id 欄位
    - zip_zone, transaction_year, transaction_season 索引
    - 中古屋包含 build_year 欄位
    
    Args:
        table_name: 資料表名稱
        engine: SQLAlchemy engine
        is_used: 是否為中古屋（需要 build_year 欄位）
    """
    # 根據交易類型決定欄位
    build_year_field = "build_year BIGINT," if is_used else ""
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        land_total_sqm DOUBLE PRECISION,
        building_total_sqm DOUBLE PRECISION,
        room_count BIGINT,
        use_zone TEXT,
        zip_zone TEXT,
        unit_price_per_sqm BIGINT,
        total_price BIGINT,
        address TEXT,
        locate_floor BIGINT,
        total_floor BIGINT,
        building_type TEXT,
        transaction_year BIGINT,
        transaction_season BIGINT,
        {build_year_field}
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION
    );
    
    -- 創建索引（如果不存在）
    CREATE INDEX IF NOT EXISTS idx_{table_name}_zip_zone ON {table_name}(zip_zone);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_transaction_year ON {table_name}(transaction_year);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_transaction_season ON {table_name}(transaction_season);
    """
    
    try:
        # SQLAlchemy 2.x 正確用法
        from sqlalchemy import inspect
        with engine.begin() as conn:  # 使用 begin() 而不是 connect()
            inspector = inspect(engine)
            table_type = "中古屋" if is_used else "預售屋"
            if not inspector.has_table(table_name):
                conn.execute(text(create_table_sql))
                log_to_gcs('INFO', f"資料表創建成功: {table_name} ({table_type})", details={
                    "constraints": ["PRIMARY KEY id", "INDEX zip_zone", "INDEX transaction_year", "INDEX transaction_season"],
                    "has_build_year": is_used
                })
            else:
                index_sqls = [
                    f"CREATE INDEX IF NOT EXISTS idx_{table_name}_zip_zone ON {table_name}(zip_zone);",
                    f"CREATE INDEX IF NOT EXISTS idx_{table_name}_transaction_year ON {table_name}(transaction_year);",
                    f"CREATE INDEX IF NOT EXISTS idx_{table_name}_transaction_season ON {table_name}(transaction_season);"
                ]
                for sql in index_sqls:
                    conn.execute(text(sql))
                log_to_gcs('INFO', f"索引檢查完成: {table_name}")
        # begin() context manager 會自動 commit
                
    except Exception as e:
        log_to_gcs('ERROR', f"資料表/索引創建失敗: {table_name}", details={
            "error": str(e),
            "traceback": traceback.format_exc()
        })
        raise

def extract_trade_data(year, season, area, trade_type, engine, **context):
    """
    處理各縣市的實價登錄資料
    
    Args:
        year: 民國年份
        season: 季度 (1-4)
        area: 地區代碼 (A, F, H, J, O)
        trade_type: 交易類型代碼 (A=中古屋, B=預售屋)
        engine: SQLAlchemy engine
        **context: Airflow context
    
    Returns:
        int: 成功載入的資料筆數
    """
    url = f"https://plvr.land.moi.gov.tw//DownloadSeason?season={year}S{season}&fileName={area}_lvr_land_{trade_type}.csv"
    
    trade_type_name = TRADE_TYPE_MAPPING[trade_type]
    area_name = AREA_CODE_NAME[area]
    TARGET_TABLE_NAME = f"real_estate_{trade_type_name}_{area_name}"
    
    log_to_gcs('INFO', f"開始處理 {area_name}, {year}S{season}, {trade_type_name}", details={
        "area": area,
        "area_name": area_name,
        "year": year,
        "season": season,
        "trade_type": trade_type,
        "target_table": TARGET_TABLE_NAME,
        "url": url
    })
    
    try:
        res = requests.get(url, timeout=30)
        
        if res.status_code != 200:
            log_to_gcs('ERROR', f"HTTP 請求失敗", details={
                "status_code": res.status_code,
                "url": url
            })
            return 0
        
        csv_data = StringIO(res.text, newline=None)
        df = pd.read_csv(csv_data, on_bad_lines='skip', engine='python', 
                        encoding='utf-8', encoding_errors='ignore')
        
        if df.empty or len(df) < 2:
            log_to_gcs('WARNING', "檔案為空或僅包含標題", details={
                "area": area_name,
                "trade_type": trade_type_name
            })
            return 0
        
        log_to_gcs('INFO', f"下載成功", details={
            "total_rows": len(df),
            "columns": df.shape[1]
        })
        
        # 檢查必要欄位
        filter_col_transfer = list(FILTER_FOR_TRANSFER_TYPE.keys())[0]
        filter_col_special = list(FILTER_FOR_SPECIAL_DEAL.keys())[0]
        filter_col_building = list(FILTER_FOR_BUILDING_TYPE.keys())[0]
        
        required_cols = list(COLUMNS_MAPPING.keys()) + [filter_col_transfer, filter_col_special, filter_col_building]
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            log_to_gcs('ERROR', "缺少必要欄位", details={
                "missing_columns": missing_cols,
                "area": area_name
            })
            return 0
        
        # 篩選 1: 只保留包含「建物」的交易標的
        filter_value_transfer = list(FILTER_FOR_TRANSFER_TYPE.values())[0]
        df = df[df[filter_col_transfer].astype(str).str.contains(filter_value_transfer, na=False, case=False)].copy()
        log_to_gcs('INFO', f"建物篩選後: {len(df)} 筆", details={
            "rows_after_filter": len(df)
        })
        
        if df.empty:
            log_to_gcs('WARNING', "沒有符合條件的建物資料")
            return 0
        
        # 篩選 2: 排除親友交易
        original_count = len(df)
        if filter_col_special in df.columns:
            filter_value_special = list(FILTER_FOR_SPECIAL_DEAL.values())[0]
            
            # 篩選出非親友交易的資料
            df = df[
                ~df[filter_col_special]
                .astype(str)
                .str.contains(filter_value_special, na=False, case=False)
            ].copy()
            
            filtered_count = original_count - len(df)
            log_to_gcs('INFO', f"排除親友交易: {filtered_count} 筆",
                      details={"original": original_count, "remaining": len(df)})
        
        if df.empty:
            log_to_gcs('WARNING', f"排除親友交易後無有效資料")
            return 0
        
        # 篩選 3: 篩選建物類型
        filter_value_building = list(FILTER_FOR_BUILDING_TYPE.values())[0]
        df = df[df[filter_col_building].astype(str).str.contains(filter_value_building, na=False, case=False)].copy()
        log_to_gcs('INFO', f"建物型態篩選後: {len(df)} 筆", details={
            "rows_after_type_filter": len(df)
        })
        
        if df.empty:
            log_to_gcs('WARNING', "沒有符合類型的建物資料")
            return 0
        
        # 合併住宅大樓和華廈
        condition_elevator = df[filter_col_building].astype(str).str.contains('住宅大樓|華廈', na=False, case=False)
        df.loc[condition_elevator, filter_col_building] = '大樓/華廈'
        
        # 統一公寓名稱
        condition_apartment = df[filter_col_building].astype(str).str.contains('公寓', na=False, case=False)
        df.loc[condition_apartment, filter_col_building] = '公寓'
        
        building_type_dist = df[filter_col_building].value_counts().to_dict()
        log_to_gcs('INFO', "建物型態分布", details=building_type_dist)
        
        # 選擇並重命名欄位
        df_clean = df[list(COLUMNS_MAPPING.keys())].rename(columns=COLUMNS_MAPPING).copy()
        
        # 數據清洗
        df_clean = df_clean.replace(r'^\s*$', np.nan, regex=True)
        
        # 數值欄位處理
        numeric_and_integer_cols = [
            "land_total_sqm", "building_total_sqm", "unit_price_per_sqm",
            "room_count", "total_price", "locate_floor", "total_floor"
        ]
        
        for col in numeric_and_integer_cols:
            if col in df_clean.columns:
                df_clean[col] = df_clean[col].astype(str).str.replace(r'[^\d.-]', '', regex=True)
                df_clean[col] = df_clean[col].replace('', np.nan)
        
        # 類型轉換
        numeric_cols = ["land_total_sqm", "building_total_sqm", "unit_price_per_sqm"]
        integer_cols = ["room_count", "total_price"]
        
        for col in numeric_cols:
            if col in df_clean.columns:
                df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
        
        for col in integer_cols:
            if col in df_clean.columns:
                df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce').astype('Int64')
        
        # 處理樓層欄位
        floor_cols = ['locate_floor', 'total_floor']
        for col in floor_cols:
            if col in df_clean.columns:
                log_to_gcs('INFO', f"轉換中文樓層數字: {col}")
                original_col_name = [k for k, v in COLUMNS_MAPPING.items() if v == col][0]
                df_clean[col] = df[original_col_name].apply(convert_chinese_floor_to_number)
                df_clean[col] = df_clean[col].astype('Int64')
        
        # 處理日期欄位
        is_used = TRADE_TYPE_MAPPING.get(trade_type) == 'used'
        df_clean = parse_date_fields(df_clean, year, season, is_used=is_used)
        
        log_to_gcs('INFO', f"日期欄位處理完成", details={
            "transaction_year": year,
            "transaction_season": season,
            "build_year_processed": is_used
        })
        
        # 處理 NULL 值
        df_clean = df_clean.where(pd.notnull(df_clean), None)
        
        # 在寫入資料前，先確保資料表存在且有正確的約束
        create_table_with_constraints(TARGET_TABLE_NAME, engine, is_used=is_used)

        # 寫入資料庫
        start_time = time.time()
        try:
            # 使用 if_exists='append' 搭配已建立的資料表結構
            # id 欄位會自動遞增，不需要在 DataFrame 中提供
            df_clean.to_sql(TARGET_TABLE_NAME, con=engine, if_exists='append', index=False)
            load_time = time.time() - start_time
            
            log_to_gcs('INFO', f"資料載入成功: {TARGET_TABLE_NAME}",
                      details={
                          "rows": len(df_clean), 
                          "time_seconds": f"{load_time:.2f}",
                          "filtered_out": original_count - len(df_clean),
                          "auto_increment_id": "enabled",
                          "indexes": ["zip_zone", "transaction_year", "transaction_season"]
                      })
            
            return len(df_clean)
            
        except Exception as e:
            tb = traceback.format_exc()
            log_to_gcs('ERROR', f"資料庫載入失敗: {TARGET_TABLE_NAME}",
                      details={"error": str(e), "traceback": tb})
            raise
        
    except Exception as e:
        log_to_gcs('ERROR', f"處理失敗: {str(e)}", details={
            "area": area_name,
            "trade_type": trade_type_name,
            "error": str(e),
            "traceback": traceback.format_exc()
        })
        return 0


def extract_hsinchucity_data(engine, year, season):
    """
    因時常為一起比較的對象，將竹北市房地產資料整合到新竹市
    
    Args:
        engine: SQLAlchemy engine
        year: 民國年份
        season: 季度
    
    Returns:
        tuple: (df_used, df_new) 處理後的中古屋和新成屋 DataFrame
    """
    # 將 import 移到函數內部，避免循環依賴
    from etls.loaders import load_real_estate_data    

    log_to_gcs('INFO', "開始載入並處理資料...")
    log_to_gcs('INFO', f"查詢資料期間: 民國 {year} 年 Q{season}")
    
    # 1. 讀取所有新成屋資料
    log_to_gcs('INFO', "[步驟 1] 讀取新成屋資料...")
    df_new_list = load_real_estate_data(engine, 'presale', year, season)
    df_new = pd.concat(df_new_list, ignore_index=True) if df_new_list else pd.DataFrame()
    log_to_gcs('INFO', f"新成屋資料總計: {len(df_new)} 筆")
    
    # 2. 讀取所有中古屋資料
    log_to_gcs('INFO', "[步驟 2] 讀取中古屋資料...")
    df_used_list = load_real_estate_data(engine, 'used', year, season)
    df_used = pd.concat(df_used_list, ignore_index=True) if df_used_list else pd.DataFrame()
    log_to_gcs('INFO', f"中古屋資料總計: {len(df_used)} 筆")
    
    # 3. 處理新竹縣竹北市資料併入新竹市
    log_to_gcs('INFO', "[步驟 3] 處理新竹縣竹北市資料...")
    
    if not df_new.empty:
        zhubei_new = df_new[(df_new['city'] == '新竹縣') & (df_new['zip_zone'] == '竹北市')].copy()
        if len(zhubei_new) > 0:
            zhubei_new['city'] = '新竹市'
            df_new = df_new[~((df_new['city'] == '新竹縣') & (df_new['zip_zone'] == '竹北市'))]
            df_new = pd.concat([df_new, zhubei_new], ignore_index=True)
            log_to_gcs('INFO', f"新成屋竹北市資料 {len(zhubei_new)} 筆已併入新竹市")
    
    if not df_used.empty:
        zhubei_used = df_used[(df_used['city'] == '新竹縣') & (df_used['zip_zone'] == '竹北市')].copy()
        if len(zhubei_used) > 0:
            zhubei_used['city'] = '新竹市'
            df_used = df_used[~((df_used['city'] == '新竹縣') & (df_used['zip_zone'] == '竹北市'))]
            df_used = pd.concat([df_used, zhubei_used], ignore_index=True)
            log_to_gcs('INFO', f"中古屋竹北市資料 {len(zhubei_used)} 筆已併入新竹市")
    
    # 4. 新增 city_display 欄位用於視覺化
    if not df_new.empty:
        df_new['city_display'] = df_new['city'].replace('新竹市', '新竹市/竹北市')
    if not df_used.empty:
        df_used['city_display'] = df_used['city'].replace('新竹市', '新竹市/竹北市')
    
    log_to_gcs('INFO', "資料處理完成")
    
    return df_used, df_new