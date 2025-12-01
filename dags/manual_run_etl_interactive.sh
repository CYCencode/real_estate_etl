#!/bin/bash
# ==========================================
# 房地產 ETL 互動式執行腳本
# 用途：在 Docker 內處理指定時間的房地產實價登錄資料
# 使用方式：
#   sudo docker exec -it api-server bash /opt/airflow/dags/run_etl_interactive.sh
# ==========================================

set -e

PROJECT_ID="real-estate-202510"

# 顏色定義
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}============================================================${NC}"
echo -e "${BLUE}        房地產 ETL 互動式執行工具${NC}"
echo -e "${BLUE}============================================================${NC}"
echo ""

# ==========================================
# 步驟 1: 詢問執行日期
# ==========================================
echo -e "${YELLOW}請輸入執行日期 (格式: YYYYMMDD)${NC}"
echo -e "範例: 20241005 = 113S3"
echo -e "提示: 留空則使用當前日期"
read -p "執行日期: " input_date

# 處理日期格式
if [ -z "$input_date" ]; then
    # 使用當前日期
    execution_date=$(date +%Y-%m-%d)
    echo -e "${GREEN}✓ 使用當前日期: $execution_date${NC}"
else
    # 驗證日期格式
    if [[ ! "$input_date" =~ ^[0-9]{8}$ ]]; then
        echo -e "${RED}❌ 錯誤: 日期格式不正確，請使用 YYYYMMDD 格式${NC}"
        exit 1
    fi
    
    # 轉換為 YYYY-MM-DD 格式
    year="${input_date:0:4}"
    month="${input_date:4:2}"
    day="${input_date:6:2}"
    execution_date="$year-$month-$day"
    
    echo -e "${GREEN}✓ 執行日期: $execution_date${NC}"
fi

echo ""

# ==========================================
# 步驟 2: 計算目標年份和季度
# ==========================================
echo -e "${BLUE}正在計算目標資料期間...${NC}"

# 使用 Python 換算民國時間、季度
read target_year target_season <<< $(python3 << PYEOF
import sys
sys.path.insert(0, '/opt/airflow/dags')
from datetime import datetime
from utils.helpers import calculate_target_year_season

execution_date = datetime.strptime("$execution_date", "%Y-%m-%d")
year, season = calculate_target_year_season(execution_date)
print(f"{year} {season}")
PYEOF
)

# 換算西元年
ad_year=$((target_year + 1911))

echo -e "${GREEN}✓ 目標資料期間: 民國 ${target_year} 年第 ${target_season} 季 (${target_year}S${target_season})${NC}"
echo -e "  對應西元: ${ad_year} 年第 ${target_season} 季"
echo ""

# ==========================================
# 步驟 3: 檢查資料庫現有資料
# ==========================================
echo -e "${BLUE}============================================================${NC}"
echo -e "${BLUE}檢查資料庫現有資料狀況${NC}"
echo -e "${BLUE}============================================================${NC}"
echo ""

# 使用 Python 查詢資料庫（比 psql 更可靠）
db_check_output=$(python3 2>&1 << PYEOF
import sys
import os
sys.path.insert(0, '/opt/airflow/dags')

from utils.database import get_pg_engine_with_retry
from sqlalchemy import text

# 資料表對應
tables = [
    ("real_estate_used_taipei", "台北市-中古屋"),
    ("real_estate_used_newtaipei", "新北市-中古屋"),
    ("real_estate_used_taoyuan", "桃園市-中古屋"),
    ("real_estate_used_hsinchucity", "新竹市-中古屋"),
    ("real_estate_used_hsinchucounty", "新竹縣-中古屋"),
    ("real_estate_presale_taipei", "台北市-預售屋"),
    ("real_estate_presale_newtaipei", "新北市-預售屋"),
    ("real_estate_presale_taoyuan", "桃園市-預售屋"),
    ("real_estate_presale_hsinchucity", "新竹市-預售屋"),
    ("real_estate_presale_hsinchucounty", "新竹縣-預售屋"),
]

try:
    context = {'task_instance': None}
    engine = get_pg_engine_with_retry(**context)
    
    print(f"期間: ${target_year}S${target_season}")
    print("┌─────────────────────────┬──────────┐")
    print("│ 資料表                  │ 筆數     │")
    print("├─────────────────────────┼──────────┤")
    
    total = 0
    with engine.connect() as conn:
        for table_name, display_name in tables:
            result = conn.execute(text(
                f"SELECT COUNT(*) FROM {table_name} "
                f"WHERE transaction_year = ${target_year} AND transaction_season = ${target_season}"
            ))
            count = result.scalar()
            total += count
            print(f"│ {display_name:23s} │ {count:8d} │")
    
    print("└─────────────────────────┴──────────┘")
    print(f"總計: {total:,} 筆")
    
    # 輸出 total 供 shell 使用（用特殊標記）
    print(f"###TOTAL:{total}###", file=sys.stderr)
    
except Exception as e:
    print(f"查詢失敗: {str(e)}")
    print("###TOTAL:0###", file=sys.stderr)
PYEOF
)

# 顯示輸出
echo "$db_check_output" | grep -v "###TOTAL:"

# 從 stderr 提取 total_count
total_count=$(echo "$db_check_output" | grep -o "###TOTAL:[0-9]*###" | grep -o "[0-9]*")
total_count=${total_count:-0}

echo ""

# ==========================================
# 步驟 4: 確認是否執行
# ==========================================
echo -e "${YELLOW}即將執行 ETL，處理以下資料:${NC}"
echo "  • 地區: 台北市、新北市、桃園市、新竹市、新竹縣"
echo "  • 類型: 中古屋、預售屋"
echo "  • 目標期間: ${target_year}S${target_season}"
echo ""

if [ $total_count -gt 0 ]; then
    echo -e "${YELLOW}⚠️  警告: 資料庫中已有 $total_count 筆資料${NC}"
    echo "   執行 ETL 會新增資料! 一批會有約 3-5 萬筆資料進到資料庫"
    echo ""
fi

read -p "是否繼續執行? (y/N): " confirm

if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
    echo -e "${YELLOW}已取消執行${NC}"
    exit 0
fi

echo ""

# ==========================================
# 步驟 5: 執行 ETL
# ==========================================
echo -e "${BLUE}============================================================${NC}"
echo -e "${BLUE}開始執行 ETL${NC}"
echo -e "${BLUE}============================================================${NC}"
echo ""

# 建立臨時 Python 腳本
cat > /tmp/run_etl_temp.py << 'PYEOF'
import sys
import os
from datetime import datetime
import time

# 設定路徑
sys.path.insert(0, '/opt/airflow/dags')

from utils.helpers import calculate_target_year_season
from utils.database import get_pg_engine_with_retry
from utils.logging import log_to_gcs
from config.settings import AREA_CODE_NAME, TRADE_TYPE_MAPPING
from etls.extractors import extract_trade_data

def main(execution_date_str):
    start_time = time.time()
    total_rows = 0
    
    try:
        # 解析執行日期
        execution_date = datetime.strptime(execution_date_str, '%Y-%m-%d')
        
        # 計算目標年份和季度
        year, season = calculate_target_year_season(execution_date)
        
        print(f"目標資料期間: {year}S{season}")
        print("")
        
        # 建立資料庫連線
        print("正在建立資料庫連線...")
        context = {'task_instance': None}
        engine = get_pg_engine_with_retry(**context)
        print("✓ 資料庫連線成功")
        print("")
        
        # 執行 ETL
        areas = list(AREA_CODE_NAME.keys())
        trade_types = list(TRADE_TYPE_MAPPING.keys())
        
        total_tasks = len(areas) * len(trade_types)
        current_task = 0
        
        for area in areas:
            for trade_type in trade_types:
                current_task += 1
                area_name = AREA_CODE_NAME[area]
                type_name = TRADE_TYPE_MAPPING[trade_type]
                
                print(f"[{current_task}/{total_tasks}] 處理中: {area_name} - {type_name}...", end=' ', flush=True)
                
                try:
                    rows = extract_trade_data(year, season, area, trade_type, engine)
                    total_rows += rows
                    print(f"✓ 完成 ({rows} 筆)")
                except Exception as e:
                    print(f"✗ 錯誤: {str(e)}")
        
        end_time = time.time()
        duration = end_time - start_time
        
        print("")
        print("=" * 60)
        print("✓ 執行完成")
        print("=" * 60)
        print(f"總筆數: {total_rows:,}")
        print(f"耗時: {duration:.2f} 秒")
        print("=" * 60)
        
        return 0
        
    except Exception as e:
        print("")
        print("=" * 60)
        print("✗ 執行失敗")
        print("=" * 60)
        print(f"錯誤: {str(e)}")
        print("=" * 60)
        return 1

if __name__ == '__main__':
    execution_date_str = sys.argv[1] if len(sys.argv) > 1 else None
    exit_code = main(execution_date_str)
    sys.exit(exit_code)
PYEOF

# 執行 Python 腳本
python3 /tmp/run_etl_temp.py "$execution_date"
etl_exit_code=$?

# 清理臨時檔案
rm -f /tmp/run_etl_temp.py

echo ""

# ==========================================
# 步驟 6: 再次檢查資料庫
# ==========================================
if [ $etl_exit_code -eq 0 ]; then
    echo -e "${BLUE}============================================================${NC}"
    echo -e "${BLUE}執行後資料庫狀況${NC}"
    echo -e "${BLUE}============================================================${NC}"
    echo ""
    
    # 使用 Python 查詢（更可靠）
    python3 << PYEOF
import sys
import os
sys.path.insert(0, '/opt/airflow/dags')

from utils.database import get_pg_engine_with_retry
from sqlalchemy import text

# 資料表對應
tables = [
    ("real_estate_used_taipei", "台北市-中古屋"),
    ("real_estate_used_newtaipei", "新北市-中古屋"),
    ("real_estate_used_taoyuan", "桃園市-中古屋"),
    ("real_estate_used_hsinchucity", "新竹市-中古屋"),
    ("real_estate_used_hsinchucounty", "新竹縣-中古屋"),
    ("real_estate_presale_taipei", "台北市-預售屋"),
    ("real_estate_presale_newtaipei", "新北市-預售屋"),
    ("real_estate_presale_taoyuan", "桃園市-預售屋"),
    ("real_estate_presale_hsinchucity", "新竹市-預售屋"),
    ("real_estate_presale_hsinchucounty", "新竹縣-預售屋"),
]

try:
    context = {'task_instance': None}
    engine = get_pg_engine_with_retry(**context)
    
    print(f"期間: ${target_year}S${target_season}")
    print("┌─────────────────────────┬──────────┐")
    print("│ 資料表                  │ 筆數     │")
    print("├─────────────────────────┼──────────┤")
    
    total = 0
    with engine.connect() as conn:
        for table_name, display_name in tables:
            result = conn.execute(text(
                f"SELECT COUNT(*) FROM {table_name} "
                f"WHERE transaction_year = ${target_year} AND transaction_season = ${target_season}"
            ))
            count = result.scalar()
            total += count
            print(f"│ {display_name:23s} │ {count:8d} │")
    
    print("└─────────────────────────┴──────────┘")
    print(f"總計: {total:,} 筆")
    
except Exception as e:
    print(f"查詢失敗: {str(e)}")
PYEOF
    
    echo ""
fi

# ==========================================
# 結束
# ==========================================
if [ $etl_exit_code -eq 0 ]; then
    echo -e "${GREEN}✓ 所有操作完成${NC}"
else
    echo -e "${RED}✗ 執行過程發生錯誤${NC}"
fi

exit $etl_exit_code