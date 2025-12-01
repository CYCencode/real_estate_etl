# api/main.py

import sys
from pathlib import Path

# 將 api 目錄加入 Python path
sys.path.insert(0, str(Path(__file__).parent))

# 將 dags 目錄加入 Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "dags"))

from pydantic import BaseModel, EmailStr, Field
from typing import Optional, List
from pipelines import generate_report_content, send_email_to_recipients
from etls import load_email_sender
from utils import get_pg_engine_with_retry, calculate_target_year_season, log_to_gcs
from fastapi import FastAPI, HTTPException, Query, Path
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
import math
import traceback
from api_config import settings
from schemas import (
    TotalPriceAndPinResponse, 
    Metadata, 
    TotalPriceAndPinItem,
    VolumeResponse,
    VolumeMetadata,
    VolumeYearlyItem,
    VolumeQuarterlyItem,
    SendReportRequest,      
    SendReportResponse,     
)
from database import DatabaseService

# 初始化 FastAPI
app = FastAPI(
    title=settings.API_TITLE,
    version=settings.API_VERSION,
    description=settings.API_DESCRIPTION,
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS 設定 (允許前端跨域請求)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 生產環境建議設定具體的 domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 初始化資料庫服務
db_config = {
    "host": settings.DB_HOST,
    "port": settings.DB_PORT,
    "database": settings.DB_NAME,
    "user": settings.DB_USER,
    "password": settings.DB_PASSWORD
}
db_service = DatabaseService(db_config)

def validate_year_range(start_year: int, end_year: int) -> None:
    """
    驗證年份範圍
    
    規則:
    1. start_year >= 113
    2. end_year <= 114
    3. start_year <= end_year
    """
    if start_year < settings.MIN_YEAR:
        raise HTTPException(
            status_code=400,
            detail=f"起始年份不能小於 {settings.MIN_YEAR}"
        )
    
    if end_year > settings.MAX_YEAR:
        raise HTTPException(
            status_code=400,
            detail=f"結束年份不能大於 {settings.MAX_YEAR}"
        )
    
    if start_year > end_year:
        raise HTTPException(
            status_code=400,
            detail="起始年份不能大於結束年份"
        )


def validate_area(city: str, area: Optional[str]) -> Optional[str]:
    """
    驗證行政區是否在該城市的白名單中
    
    特殊處理:
    - 新竹市: 原始資料不區分行政區,自動忽略 area 參數
    
    Args:
        city: 城市名稱 (中文)
        area: 行政區名稱
        
    Returns:
        驗證後的 area 值 (新竹市會返回 None)
    """
    if area is None:
        return None  # area 是選填的,沒填就不檢查
    
    # 新竹市特殊處理: 原始資料不區分行政區,忽略 area 參數
    if city == "新竹市":
        return None
    
    if city not in settings.CITY_AREAS:
        raise HTTPException(
            status_code=400,
            detail=f"系統尚未設定 {city} 的行政區資料"
        )
    
    allowed_areas = settings.CITY_AREAS[city]
    if area not in allowed_areas:
        raise HTTPException(
            status_code=400,
            detail=f"不支援的行政區。{city} 支援的行政區: {', '.join(allowed_areas)}"
        )
    
    return area

@app.get("/", tags=["Root"])
async def root():
    """API 根路徑"""
    return {
        "message": "Real Estate API",
        "version": settings.API_VERSION,
        "docs": "/docs",
        "supported_cities": list(settings.SUPPORTED_CITIES.keys())
    }

@app.get("/health", tags=["Health"])
async def health_check():
    """健康檢查端點"""
    try:
        # 測試資料庫連線
        with db_service.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}

# ==================== API 1: 查詢所有房價總價與總坪數 (預售+中古) ====================

@app.get(
    "/housetrade/total_price_and_pin/{city}",
    response_model=TotalPriceAndPinResponse,
    tags=["House Trade"],
    summary="查詢房價總價與總坪數",
    description="查詢指定時間區間內的房價總價+總坪數 (包含預售屋與中古屋)"
)
async def get_total_price_and_pin(
    city: str = Path(
        ...,
        description="縣市名稱 (例如: 台北市, 新北市, 桃園市, 新竹市, 新竹縣)",
        example="台北市"
    ),
    area: Optional[str] = Query(
        None,
        description="行政區名稱 (例如: 內湖區, 中正區)",
        example="內湖區"
    ),
    start_year: int = Query(
        113,
        ge=113,
        le=114,
        description="起始年份 (民國年)",
        example=113
    ),
    end_year: int = Query(
        114,
        ge=113,
        le=114,
        description="結束年份 (民國年)",
        example=114
    ),
    page: int = Query(
        1,
        ge=1,
        description="頁數 (從 1 開始)",
        example=1
    ),
    page_size: int = Query(
        100,
        ge=1,
        le=settings.MAX_PAGE_SIZE,
        description=f"每頁筆數 (最大 {settings.MAX_PAGE_SIZE})",
        example=100
    )
) -> TotalPriceAndPinResponse:
    """
    查詢指定條件的房價總價與總坪數
    
    - **city**: 縣市名稱 (必填)
    - **area**: 行政區名稱 (選填)
    - **start_year**: 起始年份,民國年 (預設: 113, 範圍: 113-114)
    - **end_year**: 結束年份,民國年 (預設: 114, 範圍: 113-114)
    - **page**: 頁數 (預設: 1)
    - **page_size**: 每頁筆數 (預設: 100, 最大: 500)
    
    回傳資料包含預售屋與中古屋的合併結果
    """
    
    # 驗證年份範圍
    validate_year_range(start_year, end_year)
    
    # 驗證城市名
    if city not in settings.SUPPORTED_CITIES:
        raise HTTPException(
            status_code=400,
            detail=f"不支援的城市。支援的城市: {', '.join(settings.SUPPORTED_CITIES.keys())}"
        )
    
    # 驗證行政區 (新竹市會自動返回 None)
    area = validate_area(city, area)

    city_en = settings.SUPPORTED_CITIES[city]
    
    # 驗證城市資料表是否存在
    if not db_service.validate_city(city_en):
        raise HTTPException(
            status_code=404,
            detail=f"找不到 {city} 的資料"
        )
    
    try:
        # 查詢資料
        data, total_records = db_service.get_total_price_and_pin(
            city_en=city_en,
            area=area,
            start_year=start_year,
            end_year=end_year,
            page=page,
            page_size=page_size
        )
        
        # 計算總頁數
        total_pages = math.ceil(total_records / page_size) if total_records > 0 else 1
        
        # 構建回應
        metadata = Metadata(
            city=city,
            area=area,
            start_year=start_year,
            end_year=end_year,
            total_records=total_records,
            page=page,
            page_size=page_size,
            total_pages=total_pages
        )
        
        return TotalPriceAndPinResponse(
            metadata=metadata,
            data=data
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"查詢失敗: {str(e)}"
        )

# ==================== API 2: 查詢中古屋房價總價與總坪數 ====================

@app.get(
    "/housetrade/used/total_price_and_pin/{city}",
    response_model=TotalPriceAndPinResponse,
    tags=["House Trade"],
    summary="查詢中古屋房價總價與總坪數",
    description="查詢指定時間區間內中古屋的房價總價+總坪數"
)
async def get_used_total_price_and_pin(
    city: str = Path(
        ...,
        description="縣市名稱 (例如: 台北市, 新北市, 桃園市, 新竹市, 新竹縣)",
        example="台北市"
    ),
    area: Optional[str] = Query(
        None,
        description="行政區名稱 (例如: 內湖區, 中正區)",
        example="內湖區"
    ),
    start_year: int = Query(
        113,
        ge=113,
        le=114,
        description="起始年份 (民國年)",
        example=113
    ),
    end_year: int = Query(
        114,
        ge=113,
        le=114,
        description="結束年份 (民國年)",
        example=114
    ),
    page: int = Query(
        1,
        ge=1,
        description="頁數 (從 1 開始)",
        example=1
    ),
    page_size: int = Query(
        100,
        ge=1,
        le=settings.MAX_PAGE_SIZE,
        description=f"每頁筆數 (最大 {settings.MAX_PAGE_SIZE})",
        example=100
    )
) -> TotalPriceAndPinResponse:
    """
    查詢指定條件的中古屋房價總價與總坪數
    
    - **city**: 縣市名稱 (必填)
    - **area**: 行政區名稱 (選填)
    - **start_year**: 起始年份,民國年 (預設: 113, 範圍: 113-114)
    - **end_year**: 結束年份,民國年 (預設: 114, 範圍: 113-114)
    - **page**: 頁數 (預設: 1)
    - **page_size**: 每頁筆數 (預設: 100, 最大: 1000)
    """
    
    # 驗證年份範圍
    validate_year_range(start_year, end_year)
    
    # 驗證城市名
    if city not in settings.SUPPORTED_CITIES:
        raise HTTPException(
            status_code=400,
            detail=f"不支援的城市。支援的城市: {', '.join(settings.SUPPORTED_CITIES.keys())}"
        )
    
    # 驗證行政區 (新竹市會自動返回 None)
    area = validate_area(city, area)
    
    city_en = settings.SUPPORTED_CITIES[city]
    
    try:
        # 查詢資料
        data, total_records = db_service.get_used_total_price_and_pin(
            city_en=city_en,
            area=area,
            start_year=start_year,
            end_year=end_year,
            page=page,
            page_size=page_size
        )
        
        # 計算總頁數
        total_pages = math.ceil(total_records / page_size) if total_records > 0 else 1
        
        # 構建回應
        metadata = Metadata(
            city=city,
            area=area,
            start_year=start_year,
            end_year=end_year,
            total_records=total_records,
            page=page,
            page_size=page_size,
            total_pages=total_pages
        )
        
        return TotalPriceAndPinResponse(
            metadata=metadata,
            data=data
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"查詢失敗: {str(e)}"
        )

# ==================== API 3: 查詢預售屋房價總價與總坪數 ====================

@app.get(
    "/housetrade/presale/statistics/{city}",
    response_model=TotalPriceAndPinResponse,
    tags=["House Trade"],
    summary="查詢預售屋房價總價與總坪數",
    description="查詢指定時間區間內預售屋的房價總價+總坪數"
)
async def get_presale_statistics(
    city: str = Path(
        ...,
        description="縣市名稱 (例如: 台北市, 新北市, 桃園市, 新竹市, 新竹縣)",
        example="台北市"
    ),
    area: Optional[str] = Query(
        None,
        description="行政區名稱 (例如: 內湖區, 中正區)",
        example="內湖區"
    ),
    start_year: int = Query(
        113,
        ge=113,
        le=114,
        description="起始年份 (民國年)",
        example=113
    ),
    end_year: int = Query(
        114,
        ge=113,
        le=114,
        description="結束年份 (民國年)",
        example=114
    ),
    page: int = Query(
        1,
        ge=1,
        description="頁數 (從 1 開始)",
        example=1
    ),
    page_size: int = Query(
        100,
        ge=1,
        le=settings.MAX_PAGE_SIZE,
        description=f"每頁筆數 (最大 {settings.MAX_PAGE_SIZE})",
        example=100
    )
) -> TotalPriceAndPinResponse:
    """
    查詢指定條件的預售屋房價總價與總坪數
    
    - **city**: 縣市名稱 (必填)
    - **area**: 行政區名稱 (選填)
    - **start_year**: 起始年份,民國年 (預設: 113, 範圍: 113-114)
    - **end_year**: 結束年份,民國年 (預設: 114, 範圍: 113-114)
    - **page**: 頁數 (預設: 1)
    - **page_size**: 每頁筆數 (預設: 100, 最大: 1000)
    """
    
    # 驗證年份範圍
    validate_year_range(start_year, end_year)
    
    # 驗證城市名
    if city not in settings.SUPPORTED_CITIES:
        raise HTTPException(
            status_code=400,
            detail=f"不支援的城市。支援的城市: {', '.join(settings.SUPPORTED_CITIES.keys())}"
        )
    
    # 驗證行政區 (新竹市會自動返回 None)
    area = validate_area(city, area)
    
    city_en = settings.SUPPORTED_CITIES[city]
    
    try:
        # 查詢資料
        data, total_records = db_service.get_presale_statistics(
            city_en=city_en,
            area=area,
            start_year=start_year,
            end_year=end_year,
            page=page,
            page_size=page_size
        )
        
        # 計算總頁數
        total_pages = math.ceil(total_records / page_size) if total_records > 0 else 1
        
        # 構建回應
        metadata = Metadata(
            city=city,
            area=area,
            start_year=start_year,
            end_year=end_year,
            total_records=total_records,
            page=page,
            page_size=page_size,
            total_pages=total_pages
        )
        
        return TotalPriceAndPinResponse(
            metadata=metadata,
            data=data
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"查詢失敗: {str(e)}"
        )

# ==================== API 4: 查詢成交量統計 ====================

@app.get(
    "/housetrade/volume/{city}",
    response_model=VolumeResponse,
    tags=["House Trade"],
    summary="查詢成交量統計",
    description="查詢指定時間區間內的成交總數，可以選擇用年/季、建物類型分組計算"
)
async def get_volume_statistics(
    city: str = Path(
        ...,
        description="縣市名稱 (例如: 台北市, 新北市, 桃園市, 新竹市, 新竹縣)",
        example="台北市"
    ),
    area: Optional[str] = Query(
        None,
        description="行政區名稱 (例如: 內湖區, 中正區)",
        example="內湖區"
    ),
    start_year: int = Query(
        113,
        ge=113,
        le=114,
        description="起始年份 (民國年)",
        example=113
    ),
    end_year: int = Query(
        114,
        ge=113,
        le=114,
        description="結束年份 (民國年)",
        example=114
    ),
    group_by: str = Query(
        "year",
        regex="^(year|quarter)$",
        description="分組單位: year (年) 或 quarter (季)",
        example="year"
    ),
    building_type: Optional[str] = Query(
        None,
        description="建物類型: 大樓/華廈、公寓、透天厝 (沒有篩選則顯示全部)",
        example=None
    )
) -> VolumeResponse:
    """
    查詢指定條件的成交量統計
    
    - **city**: 縣市名稱 (必填)
    - **area**: 行政區名稱 (選填)
    - **start_year**: 起始年份,民國年 (預設: 113, 範圍: 113-114)
    - **end_year**: 結束年份,民國年 (預設: 114, 範圍: 113-114)
    - **group_by**: 分組方式 - year (年) 或 quarter (季) (預設: year)
    - **building_type**: 建物類型篩選 (選填)
    
    回傳依照指定分組方式的成交量統計
    """
    
    # 驗證年份範圍
    validate_year_range(start_year, end_year)
    
    # 驗證城市名
    if city not in settings.SUPPORTED_CITIES:
        raise HTTPException(
            status_code=400,
            detail=f"不支援的城市。支援的城市: {', '.join(settings.SUPPORTED_CITIES.keys())}"
        )
    
    # 驗證行政區 (新竹市會自動返回 None)
    area = validate_area(city, area)
    
    city_en = settings.SUPPORTED_CITIES[city]
    
    # 驗證城市資料表是否存在
    if not db_service.validate_city(city_en):
        raise HTTPException(
            status_code=404,
            detail=f"找不到 {city} 的資料"
        )
    
    try:
        # 查詢資料
        data, total_volume = db_service.get_volume_statistics(
            city_en=city_en,
            area=area,
            start_year=start_year,
            end_year=end_year,
            group_by=group_by,
            building_type=building_type
        )
        
        # 構建回應
        metadata = VolumeMetadata(
            city=city,
            area=area,
            start_year=start_year,
            end_year=end_year,
            group_by=group_by,
            building_type=building_type,
            total_volume=total_volume
        )
        
        return VolumeResponse(
            metadata=metadata,
            data=data
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"查詢失敗: {str(e)}"
        )

@app.post(
    "/housetrade/report/send",
    response_model=SendReportResponse,
    tags=["Report"],
    summary="發送房地產季報",
    description="生成並發送指定期間的房地產季報給指定收件人"
)
async def send_quarterly_report(request: SendReportRequest):
    """
    觸發房地產季報發送
    
    - **recipients**: 收件人 email 清單（必填）
    - **year**: 報告年份，民國年（選填，不指定則使用最新一期）
    - **season**: 報告季度 1-4（選填，不指定則使用最新一期）
    
    寄件人由系統從資料庫讀取（由 developer 管理）
    """
    
    try:
        # 1. 決定報告期間
        if request.year is None or request.season is None:
            year, season = calculate_target_year_season()  
            log_to_gcs('INFO', f"未指定期間，使用最新一期: {year}S{season}")
        else:
            year = request.year
            season = request.season
            log_to_gcs('INFO', f"使用指定期間: {year}S{season}")
        
        # 2. 建立資料庫連線
        engine = get_pg_engine_with_retry()  
        
        # 3. 載入寄件人（從資料庫）
        sender = load_email_sender(engine)
        if not sender:
            raise HTTPException(
                status_code=500,
                detail="系統未設定寄件人，請聯絡管理員"
            )
        
        # 4. 生成報告內容
        log_to_gcs('INFO', f"開始生成 {year}S{season} 報告...")
        report_data = generate_report_content(year, season, engine)
        
        # 5. 發送郵件
        log_to_gcs('INFO', f"開始發送郵件給 {len(request.recipients)} 位收件人...")
        result = send_email_to_recipients(
            html_content=report_data['html_content'],
            all_image_urls=report_data['all_image_urls'],
            recipients=request.recipients,
            sender=sender,
            year=year,
            season=season
        )
        
        return SendReportResponse(
            success=result['success_count'] > 0,
            message=f"成功發送 {result['success_count']}/{len(request.recipients)} 封郵件",
            year=year,
            season=season,
            success_count=result['success_count'],
            failed_recipients=result['failed_recipients']
        )
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        log_to_gcs('ERROR', f"發送報告失敗: {str(e)}", details=traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"發送失敗: {str(e)}")
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,  # 開發環境使用,生產環境改為 False
        log_level="info"
    )
