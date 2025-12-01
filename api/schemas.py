#api/schemas.py
"""
Pydantic Models for API Request/Response
"""
from typing import List, Optional, Union 
from pydantic import BaseModel, Field, EmailStr

class TotalPriceAndPinItem(BaseModel):
    """單筆房價總價與總坪數資料"""
    total_price: int = Field(..., description="房屋總價 (元)")
    total_pin: float = Field(..., description="總坪數 (坪)")
    
    class Config:
        json_schema_extra = {
            "example": {
                "total_price": 8500000,
                "total_pin": 19.5
            }
        }

class Metadata(BaseModel):
    """回應的 metadata"""
    city: str = Field(..., description="縣市名稱")
    area: Optional[str] = Field(None, description="行政區名稱")
    start_year: int = Field(..., description="起始年份 (民國年)")
    end_year: int = Field(..., description="結束年份 (民國年)")
    total_records: int = Field(..., description="符合條件的總筆數")
    page: int = Field(..., description="當前頁數")
    page_size: int = Field(..., description="每頁筆數")
    total_pages: int = Field(..., description="總頁數")

class TotalPriceAndPinResponse(BaseModel):
    """API 回應格式"""
    metadata: Metadata
    data: List[TotalPriceAndPinItem]
    
    class Config:
        json_schema_extra = {
            "example": {
                "metadata": {
                    "city": "台北市",
                    "area": None,
                    "start_year": 113,
                    "end_year": 114,
                    "total_records": 2,
                    "page": 1,
                    "page_size": 100,
                    "total_pages": 1
                },
                "data": [
                    {"total_price": 8500000, "total_pin": 19.5},
                    {"total_price": 10000000, "total_pin": 28.3}
                ]
            }
        }

class VolumeYearlyItem(BaseModel):
    """年度成交量資料"""
    year: int = Field(..., description="年份 (民國年)")
    volume: int = Field(..., description="成交數量")
    
    class Config:
        json_schema_extra = {
            "example": {
                "year": 114,
                "volume": 2156
            }
        }

class VolumeQuarterlyItem(BaseModel):
    """季度成交量資料"""
    year: int = Field(..., description="年份 (民國年)")
    quarter: int = Field(..., description="季度 (1-4)")
    volume: int = Field(..., description="成交數量")
    
    class Config:
        json_schema_extra = {
            "example": {
                "year": 114,
                "quarter": 1,
                "volume": 520
            }
        }

class VolumeMetadata(BaseModel):
    """成交量 metadata"""
    city: str = Field(..., description="縣市名稱")
    area: Optional[str] = Field(None, description="行政區名稱")
    start_year: int = Field(..., description="起始年份 (民國年)")
    end_year: int = Field(..., description="結束年份 (民國年)")
    group_by: str = Field(..., description="分組方式: year 或 quarter")
    building_type: Optional[str] = Field(None, description="建物類型")
    total_volume: int = Field(..., description="總成交量")

class VolumeResponse(BaseModel):
    """API 回應格式 - 成交量統計"""
    metadata: VolumeMetadata
    data: Union[List[VolumeYearlyItem], List[VolumeQuarterlyItem]]
    
    class Config:
        json_schema_extra = {
            "example": {
                "metadata": {
                    "city": "台北市",
                    "area": "內湖區",
                    "start_year": 113,
                    "end_year": 114,
                    "group_by": "year",
                    "building_type": None,
                    "total_volume": 8947
                },
                "data": [
                    {"year": 111, "volume": 2156},
                    {"year": 113, "volume": 2387},
                    {"year": 113, "volume": 2204},
                    {"year": 114, "volume": 2200}
                ]
            }
        }
class SendReportRequest(BaseModel):
    """發送報告請求"""
    recipients: List[EmailStr] = Field(
        ...,
        description="收件人 email 清單",
        min_items=1,
        max_items=50,
        example=["user1@example.com", "user2@example.com"]
    )
    year: Optional[int] = Field(
        None,
        ge=113,
        le=114,
        description="報告年份(民國年)，不指定則使用最新一期",
        example=113
    )
    season: Optional[int] = Field(
        None,
        ge=1,
        le=4,
        description="報告季度(1-4)，不指定則使用最新一期",
        example=4
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "recipients": ["analyst@company.com", "manager@company.com"],
                "year": 113,
                "season": 4
            }
        }

class SendReportResponse(BaseModel):
    """發送報告回應"""
    success: bool = Field(..., description="是否成功")
    message: str = Field(..., description="執行結果訊息")
    year: int = Field(..., description="報告年份(民國年)")
    season: int = Field(..., description="報告季度")
    success_count: int = Field(..., description="成功發送數量")
    failed_recipients: List[dict] = Field(default=[], description="失敗的收件人清單")
    
    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "message": "成功發送 2/2 封郵件",
                "year": 113,
                "season": 4,
                "success_count": 2,
                "failed_recipients": []
            }
        }