# etls/transformers.py
"""資料轉換清洗模組"""

import pandas as pd

from utils.logging import log_to_gcs


def convert_chinese_floor_to_number(floor_str):
    """
    將中文樓層數字轉換為阿拉伯數字
    
    Args:
        floor_str: 中文樓層字串，例如 "八層"、"十二層"、"二十三層"、"地下一層"
    
    Returns:
        int or None: 轉換後的數字，無法轉換則返回 None
        
    Examples:
        >>> convert_chinese_floor_to_number("八層")
        8
        >>> convert_chinese_floor_to_number("十二層")
        12
        >>> convert_chinese_floor_to_number("地下一層")
        -1
    """
    if pd.isna(floor_str) or floor_str == '':
        return None

    floor_str = str(floor_str).strip()
    floor_str = floor_str.replace('層', '')

    try:
        return int(floor_str)
    except:
        pass

    is_basement = False
    if '地下' in floor_str:
        is_basement = True
        floor_str = floor_str.replace('地下', '')

    chinese_to_arabic = {
        '零': 0, '一': 1, '二': 2, '三': 3, '四': 4,
        '五': 5, '六': 6, '七': 7, '八': 8, '九': 9,
        '十': 10, '百': 100
    }

    if not floor_str:
        return None

    if not any(char in chinese_to_arabic for char in floor_str):
        return None

    result = 0
    temp = 0
    unit = 1

    for char in floor_str:
        if char in chinese_to_arabic:
            num = chinese_to_arabic[char]

            if num == 10:
                if temp == 0:
                    temp = 10
                else:
                    temp *= 10
                unit = 10
            elif num == 100:
                if temp == 0:
                    temp = 100
                else:
                    temp *= 100
                unit = 100
            else:
                if unit == 10:
                    result += temp
                    temp = num
                    unit = 1
                elif unit == 100:
                    result += temp
                    temp = num
                    unit = 1
                else:
                    temp = num

    result += temp

    if is_basement:
        result = -result

    return result if result != 0 else None


def parse_date_fields(df, year, season, is_used=True):
    """
    解析日期欄位
    
    新增 transaction_year 和 transaction_season 欄位（使用下載時的年份和季度）
    build_date: "1050600" -> build_year=105 (僅中古屋，不再處理月份)

    Args:
        df: DataFrame
        year: int, 民國年份（例如 114）
        season: int, 季度（1-4）
        is_used: bool, True=中古屋(處理建築完成日期), False=預售屋(不處理建築完成日期)

    Returns:
        DataFrame: 處理後的 DataFrame
    """
    df_processed = df.copy()

    # 新增交易年份和季度欄位（使用參數傳入的值）
    df_processed['transaction_year'] = year
    df_processed['transaction_season'] = season

    # 處理建築完成年月 (僅中古屋需要，且只取年份)
    if is_used and 'build_date' in df_processed.columns:
        # 轉換為字串並填充
        df_processed['build_date'] = df_processed['build_date'].astype(str).str.zfill(7)

        # 只提取年份，使用 errors='coerce' 處理異常值
        df_processed['build_year'] = pd.to_numeric(
            df_processed['build_date'].str[:3], errors='coerce'
        ).astype('Int64')

        # 移除原始欄位
        df_processed = df_processed.drop(columns=['build_date'])
    elif not is_used and 'build_date' in df_processed.columns:
        # 預售屋：直接移除 build_date 欄位（不處理）
        df_processed = df_processed.drop(columns=['build_date'])

    return df_processed