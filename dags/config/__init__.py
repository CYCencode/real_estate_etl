"""配置模組 - 所有共用設定"""

from config.settings import (
    # DAG 設定
    default_args,
    GCS_BUCKET_NAME,
    CONTROL_VM_IP,
    
    # Email 設定
    SMTP_HOST,
    SMTP_PORT,
    
    # 地區對應
    AREA_CODE_NAME,
    CITY_NAME_MAPPING,
    TRADE_TYPE_MAPPING,
    TABLES,
    
    # 資料欄位
    COLUMNS_MAPPING,
    FILTER_FOR_TRANSFER_TYPE,
    FILTER_FOR_SPECIAL_DEAL,
    FILTER_FOR_BUILDING_TYPE,
    
    # 視覺化設定
    COLORS,
    ZONE_ORDER,
    BUILDING_TYPE_ORDER,  # ✅ 加入這個
    REPORT_CITIES,
    CITY_DISPLAY_TO_CODE,
    
    # 圖表樣式
    FIGURE_SIZE_WIDE,
    FIGURE_SIZE_MEDIUM,
    FIGURE_SIZE_LARGE,
    FIGURE_SIZE_MAP,
    SUBPLOT_LEFT,
    SUBPLOT_RIGHT,
    LEGEND_RIGHT,
    LEGEND_RIGHT_LOWER,
    CHART_DPI,
    LABEL_THRESHOLD,
    
    # 散點圖設定
    PRICE_COLORS,
    SIZE_MAPPING,
    
    # 資料級距
    AGE_BINS,
    AGE_LABELS,
    AREA_BINS,
    AREA_LABELS,
    PRICE_BINS_USED,
    PRICE_LABELS_USED,
    PRICE_BINS_NEW,
    PRICE_LABELS_NEW,
    
    # 地理設定
    CITY_BOUNDS,
    MAX_POINTS,
)

__all__ = [
    # DAG 設定
    'default_args',
    'GCS_BUCKET_NAME',
    'CONTROL_VM_IP',
    
    # Email 設定
    'SMTP_HOST',
    'SMTP_PORT',
    
    # 地區對應
    'AREA_CODE_NAME',
    'CITY_NAME_MAPPING',
    'TRADE_TYPE_MAPPING',
    'TABLES',
    
    # 資料欄位
    'COLUMNS_MAPPING',
    'FILTER_FOR_TRANSFER_TYPE',
    'FILTER_FOR_SPECIAL_DEAL',
    'FILTER_FOR_BUILDING_TYPE',
    
    # 視覺化設定
    'COLORS',
    'ZONE_ORDER',
    'BUILDING_TYPE_ORDER', 
    'REPORT_CITIES',
    'CITY_DISPLAY_TO_CODE',
    
    # 圖表樣式
    'FIGURE_SIZE_WIDE',
    'FIGURE_SIZE_MEDIUM',
    'FIGURE_SIZE_LARGE',
    'FIGURE_SIZE_MAP',
    'SUBPLOT_LEFT',
    'SUBPLOT_RIGHT',
    'LEGEND_RIGHT',
    'LEGEND_RIGHT_LOWER',
    'CHART_DPI',
    'LABEL_THRESHOLD',
    
    # 散點圖設定
    'PRICE_COLORS',
    'SIZE_MAPPING',
    
    # 資料級距
    'AGE_BINS',
    'AGE_LABELS',
    'AREA_BINS',
    'AREA_LABELS',
    'PRICE_BINS_USED',
    'PRICE_LABELS_USED',
    'PRICE_BINS_NEW',
    'PRICE_LABELS_NEW',
    
    # 地理設定
    'CITY_BOUNDS',
    'MAX_POINTS',
]