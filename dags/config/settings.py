"""所有共用設定"""

from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import numpy as np

# ==================== AIRFLOW DAG 設定 ====================
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=30), 
}

# ==================== GCS 設定 ====================
GCS_BUCKET_NAME = 'real-estate-202510-etl-logs'
CONTROL_VM_IP = '35.223.246.64'

# ==================== Email 設定 ====================
SMTP_HOST = 'smtp.gmail.com'
SMTP_PORT = 587  
# ==================== 地區與交易類型對應 ====================
# 地區代碼對應表 (code -> 英文名稱，小寫)
AREA_CODE_NAME = {
    'A': 'taipei', 
    'F': 'newtaipei', 
    'H': 'taoyuan', 
    'J': 'hsinchucounty', 
    'O': 'hsinchucity'
}

# 交易類型對應表 (code -> 類型名稱)
TRADE_TYPE_MAPPING = {
    'A': 'used',      # 中古屋
    'B': 'presale'    # 預售屋
}

# 動態生成所有資料表名稱
TABLES = [f'real_estate_{trade_type}_{area}' 
          for trade_type in TRADE_TYPE_MAPPING.values() 
          for area in AREA_CODE_NAME.values()]

# ==================== 資料欄位設定 ====================
# 欄位對應表
COLUMNS_MAPPING = {
    "土地移轉總面積平方公尺": "land_total_sqm",
    "建物移轉總面積平方公尺": "building_total_sqm",
    "建物現況格局-房": "room_count",
    "主要用途": "use_zone",
    "鄉鎮市區": "zip_zone",
    "建築完成年月": "build_date",
    "單價元平方公尺": "unit_price_per_sqm",
    "總價元": "total_price",
    "土地位置建物門牌": "address",
    "移轉層次": "locate_floor",
    "總樓層數": "total_floor",
    "建物型態": "building_type",
}

# ==================== 資料篩選條件 ====================
# 篩選：只保留建物交易
FILTER_FOR_TRANSFER_TYPE = {
    "交易標的": "建物"
}

# 篩選：排除親友交易
FILTER_FOR_SPECIAL_DEAL = {
    "備註": "親友、員工、共有人或其他特殊關係間之交易"
}

# 篩選：保留的建物型態
FILTER_FOR_BUILDING_TYPE = {
    "建物型態": "住宅大樓|華廈|公寓|透天"
}

# ==================== 顯示名稱對應 ====================
# 縣市中文名稱對應
CITY_NAME_MAPPING = {
    'taipei': '台北市',
    'newtaipei': '新北市',
    'taoyuan': '桃園市',
    'hsinchucounty': '新竹縣',
    'hsinchucity': '新竹市'
}

# ==================== 報表視覺化設定 ====================
# 定義顏色配置（黃、橙、紅、綠、淺藍、深藍）
COLORS = ['#FFE699', '#FFB366', '#FF6B6B', '#90EE90', '#87CEEB', '#4682B4']

# 定義區域順序（從上到下）
ZONE_ORDER = ['台北市', '新北市', '新竹市/竹北市', '桃園市', '新竹縣']

# 定義建物類型順序
BUILDING_TYPE_ORDER = ['公寓', '透天厝', '大樓/華廈']

# 縣市列表（用於報告生成）
REPORT_CITIES = [
    ('台北市', '台北市'),
    ('新北市', '新北市'),
    ('桃園市', '桃園市'),
    ('新竹市/竹北市', '新竹市'),
    ('新竹縣', '新竹縣')
]

# 縣市名稱對應（中文 -> 英文）
CITY_DISPLAY_TO_CODE = {
    '台北市': 'Taipei',
    '新北市': 'NewTaipei',
    '桃園市': 'Taoyuan',
    '新竹縣': 'HsinchuCounty',
    '新竹市/竹北市': 'HsinchuCity'
}

# ==================== 圖表樣式設定 ====================

# 圖表尺寸
FIGURE_SIZE_WIDE = (18, 9)      # 寬版圖表（堆疊橫條圖）
FIGURE_SIZE_MEDIUM = (14, 9)    # 中版圖表（屋齡圖）
FIGURE_SIZE_LARGE = (24, 8)     # 大版圖表（箱型圖）
FIGURE_SIZE_MAP = (14, 12)      # 地圖尺寸

# 子圖位置（左、下、寬、高）
SUBPLOT_LEFT = [0.08, 0.15, 0.38, 0.75]
SUBPLOT_RIGHT = [0.54, 0.15, 0.38, 0.75]
LEGEND_RIGHT = [0.93, 0.35, 0.05, 0.35]
LEGEND_RIGHT_LOWER = [0.93, 0.20, 0.05, 0.35]

# 圖表 DPI
CHART_DPI = 150

# 堆疊橫條圖顯示標籤閾值（百分比低於此值不顯示）
LABEL_THRESHOLD = 5

# ==================== 散點圖顏色設定 ====================

# 價格顏色映射（四分位距）
PRICE_COLORS = {
    1: '#1A237E',    # 深靛藍 (0~Q1) - 最便宜
    2: '#D32F2F',    # 鮮紅 (Q1~Q2)
    3: '#FF6F00',    # 鮮橘 (Q2~Q3)
    4: '#FDD835',    # 亮黃 (Q3~MAX) - 最貴
    0: '#BDBDBD'     # 灰色 (未知)
}

# 坪數大小映射（四分位距）
SIZE_MAPPING = {
    1: 50,     # 0~Q1
    2: 100,    # Q1~Q2
    3: 200,    # Q2~Q3
    4: 350,    # Q3~MAX
    0: 100     # 未知
}

# ==================== 資料級距設定 ====================

# 屋齡分級
AGE_BINS = [0, 5, 10, 20, 30, 40, np.inf]
AGE_LABELS = ['5年內', '5-10年', '10-20年', '20-30年', '30-40年', '40年以上']

# 建坪分級（坪數）
AREA_BINS = [0, 20, 30, 40, 50, 60, np.inf]
AREA_LABELS = ['20坪以下', '20-30坪', '30-40坪', '40-50坪', '50-60坪', '60坪以上']

# 中古屋總價分級
PRICE_BINS_USED = [0, 5000000, 10000000, 15000000, 20000000, 30000000, np.inf]
PRICE_LABELS_USED = ['500萬以下', '500-1000萬', '1000-1500萬', '1500-2000萬', '2000-3000萬', '3000萬以上']

# 新成屋總價分級
PRICE_BINS_NEW = [0, 10000000, 15000000, 20000000, 30000000, 50000000, np.inf]
PRICE_LABELS_NEW = ['1000萬以下', '1000-1500萬', '1500-2000萬', '2000-3000萬', '3000-5000萬', '5000萬以上']

# ==================== Matplotlib 中文字體設定 ====================
import logging

def setup_chinese_font():
    """設定 Matplotlib 中文字體（載入繁體中文 .otf 字體）"""
    try:
        import os
        
        # 確保 Matplotlib 快取目錄可寫入
        mpl_config_dir = os.environ.get('MPLCONFIGDIR', '/tmp/matplotlib_config')
        os.makedirs(mpl_config_dir, exist_ok=True)
        logging.info(f"📁 Matplotlib 快取目錄: {mpl_config_dir}")
        
        # 載入繁體中文 .otf 字體（避免 Matplotlib 無法識別 .ttc 多語言字體）
        otf_paths = [
            '/usr/share/fonts/truetype/noto/NotoSansCJKtc-Regular.otf',
            '/usr/share/fonts/truetype/noto/NotoSansCJKtc-Bold.otf',
        ]
        
        loaded_count = 0
        for font_path in otf_paths:
            if os.path.exists(font_path):
                try:
                    fm.fontManager.addfont(font_path)
                    loaded_count += 1
                    logging.info(f"載入 .otf 字體: {font_path}")
                except Exception as e:
                    logging.warning(f"無法載入 {font_path}: {e}")
        
        if loaded_count > 0:
            # 強制重新掃描（不使用快取）
            fm._load_fontmanager(try_read_cache=False)
            logging.info(f"強制重新掃描字體（載入了 {loaded_count} 個 .otf 檔案）")
        
        # 檢查可用字體
        all_fonts = [f.name for f in fm.fontManager.ttflist]
        cjk_fonts = sorted(set([f for f in all_fonts if 'CJK' in f]))
        tc_fonts = [f for f in cjk_fonts if 'TC' in f or 'tc' in f.lower()]
        
        logging.info(f"CJK 字體總數: {len(cjk_fonts)}")
        logging.info(f"TC 字體總數: {len(tc_fonts)}")
        
        if tc_fonts:
            logging.info(f"可用 TC 字體: {tc_fonts}")
        
        # 選擇字體
        if tc_fonts:
            selected_font = tc_fonts[0]
            logging.info(f"使用繁體中文字體: {selected_font}")
        elif cjk_fonts:
            selected_font = cjk_fonts[0]
            logging.warning(f"未找到 TC 字體，fallback 到: {selected_font}")
        else:
            selected_font = 'DejaVu Sans'
            logging.error("未找到任何 CJK 字體，中文將顯示為方框")
        
        # 設定 Matplotlib
        plt.rcParams['font.sans-serif'] = [selected_font, 'sans-serif']
        plt.rcParams['axes.unicode_minus'] = False
        plt.rcParams['figure.dpi'] = 100
        
        logging.info(f"字體設定完成: {plt.rcParams['font.sans-serif']}")
        
    except Exception as e:
        logging.error(f"字體設定失敗: {e}")
        import traceback
        logging.error(traceback.format_exc())
        
        # Fallback
        plt.rcParams['font.sans-serif'] = ['DejaVu Sans']
        plt.rcParams['axes.unicode_minus'] = False
        plt.rcParams['figure.dpi'] = 100

# 執行字體設定
setup_chinese_font()

# ==================== 地理範圍設定 ====================
# 各縣市的合理經緯度範圍（用於過濾異常座標）
CITY_BOUNDS = {
    '台北市': {'lat': (24.95, 25.20), 'lon': (121.45, 121.65)},
    '新北市': {'lat': (24.60, 25.30), 'lon': (121.30, 122.00)},
    '桃園市': {'lat': (24.70, 25.30), 'lon': (120.90, 121.50)},
    '新竹市/竹北市': {'lat': (24.70, 24.90), 'lon': (120.90, 121.10)},
    '新竹縣': {'lat': (24.40, 25.00), 'lon': (120.70, 121.30)},
}

# 單一縣市散點圖樣本採集數量
MAX_POINTS = 5000
