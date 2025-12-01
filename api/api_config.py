"""
API Configuration - 整合現有環境變數
"""
import os

class Settings:
    # API 設定
    API_TITLE = "Real Estate API"
    API_VERSION = "1.0.0"
    API_DESCRIPTION = "台灣房地產交易資料查詢 API"
    
    # Database 設定 - 讀取現有的環境變數
    DB_HOST: str = os.getenv("PG_HOST", "10.19.112.3")
    DB_PORT: int = int(os.getenv("PG_PORT", "5432"))
    DB_NAME: str = "postgres"
    DB_USER: str = os.getenv("PG_USER", "postgres")
    DB_PASSWORD: str = os.getenv("PG_PASSWORD", "")
    
    # 年份範圍限制
    MIN_YEAR = 113  # 最早年份 (民國 113 年)
    MAX_YEAR = 114  # 最晚年份 (民國 114 年)
    
    # Pagination 設定
    DEFAULT_PAGE_SIZE = 100
    MAX_PAGE_SIZE = 500
    
    # 支援的城市列表
    SUPPORTED_CITIES = {
        "台北市": "taipei",
        "新北市": "newtaipei",
        "桃園市": "taoyuan",
        "新竹市": "hsinchucity",
        "新竹縣": "hsinchucounty"
    }
    
    # 各城市支援的行政區 (區域白名單)
    # 政府實價登錄原始資料中，新竹市的區域統一為新竹市，若搜尋特定區域，直接回傳所有新竹市資料
    CITY_AREAS = {
        "台北市": [
            "中正區", "大同區", "中山區", "松山區", "大安區", "萬華區",
            "信義區", "士林區", "北投區", "內湖區", "南港區", "文山區"
        ],
        "新北市": [
            "板橋區", "三重區", "中和區", "永和區", "新莊區", "新店區",
            "樹林區", "鶯歌區", "三峽區", "淡水區", "汐止區", "瑞芳區",
            "土城區", "蘆洲區", "五股區", "泰山區", "林口區", "深坑區",
            "石碇區", "坪林區", "三芝區", "石門區", "八里區", "平溪區",
            "雙溪區", "貢寮區", "金山區", "萬里區", "烏來區"
        ],
        "桃園市": [
            "桃園區", "中壢區", "平鎮區", "八德區", "楊梅區", "蘆竹區",
            "大溪區", "龍潭區", "龜山區", "大園區", "觀音區", "新屋區", "復興區"
        ],
        "新竹市": [
            "新竹市"
        ],
        "新竹縣": [
            "竹北市", "竹東鎮", "新埔鎮", "關西鎮", "湖口鄉", "新豐鄉",
            "峨眉鄉", "寶山鄉", "北埔鄉", "芎林鄉", "橫山鄉", "尖石鄉", "五峰鄉"
        ]
    }
    
    @property
    def database_url(self) -> str:
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

settings = Settings()