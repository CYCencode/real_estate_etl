"""
Database Service for Real Estate Data - Optimized Version
優化重點:
1. 移除 address LIKE 查詢，僅使用 zip_zone (有索引)
2. 遵循 SARG 原則 - 所有 WHERE 條件都可使用索引
3. 提早返回 (early return) - 避免無效運算
4. 優化 UNION ALL 查詢 - 避免不必要的 subquery
5. 針對小資料量 (50萬筆/季3萬) 優化
6. 新竹市資料不支援區域過濾,自動忽略 area 參數
"""
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import List, Dict, Optional, Tuple
from contextlib import contextmanager

class DatabaseService:
    def __init__(self, db_config: Dict[str, str]):
        self.db_config = db_config
    
    @contextmanager
    def get_connection(self):
        """建立資料庫連線的 context manager"""
        conn = psycopg2.connect(**self.db_config)
        try:
            yield conn
        finally:
            conn.close()
    
    def _get_table_names(self, city_en: str, table_type: Optional[str] = None) -> List[str]:
        """
        取得指定城市的資料表名稱
        
        Args:
            city_en: 城市英文代碼 (例如: taipei, newtaipei)
            table_type: 資料表類型 ('presale', 'used', None=全部)
            
        Returns:
            資料表名稱列表
        """
        if table_type == 'presale':
            return [f"real_estate_presale_{city_en}"]
        elif table_type == 'used':
            return [f"real_estate_used_{city_en}"]
        else:
            return [
                f"real_estate_presale_{city_en}",
                f"real_estate_used_{city_en}"
            ]
    
    def _should_ignore_area(self, city_en: str) -> bool:
        """
        判斷是否應該忽略 area 參數
        
        新竹市的原始資料不區分行政區,統一標記為"新竹市"
        因此查詢時應忽略 area 參數,避免無效過濾
        
        Args:
            city_en: 城市英文代碼
            
        Returns:
            True: 應忽略 area 參數
            False: 可使用 area 參數過濾
        """
        return city_en == "hsinchucity"
    
    def _convert_sqm_to_pin(self, sqm: float) -> float:
        """
        將平方公尺轉換為坪 (1坪 = 3.30579平方公尺)
        Returns null on null input
        """
        if sqm is None:
            return None
        return round(sqm / 3.30579, 2)
    
    def get_total_price_and_pin(
        self,
        city_en: str,
        area: Optional[str] = None,
        start_year: int = 113,
        end_year: int = 114,
        page: int = 1,
        page_size: int = 100
    ) -> Tuple[List[Dict], int]:
        """
        查詢房價總價與總坪數 (預售+中古)
        
        優化說明:
        1. 只使用 zip_zone 查詢 (有索引)
        2. WHERE 條件符合 SARG 原則
        3. 直接在各表查詢後 UNION ALL，避免 subquery
        4. 新竹市自動忽略 area 參數 (原始資料限制)
        
        Args:
            city_en: 城市英文代碼
            area: 行政區名稱 (選填) - 對應 zip_zone
            start_year: 起始年份 (民國年)
            end_year: 結束年份 (民國年)
            page: 頁數
            page_size: 每頁筆數
            
        Returns:
            (資料列表, 總筆數)
        """
        # 新竹市不支援區域過濾
        if self._should_ignore_area(city_en):
            area = None
        
        tables = self._get_table_names(city_en)
        
        # 構建 UNION ALL 查詢 - 直接查詢需要的欄位
        union_queries = []
        params = []
        
        for table in tables:
            # 基本查詢 - 只選取需要的欄位，減少記憶體使用
            query = f"""
                SELECT 
                    total_price,
                    building_total_sqm
                FROM {table}
                WHERE transaction_year BETWEEN %s AND %s
            """
            params.extend([start_year, end_year])
            
            # 只使用有索引的 zip_zone
            if area:
                query += " AND zip_zone = %s"
                params.append(area)
            
            union_queries.append(query)
        
        # 組合查詢 - 加上排序和分頁
        full_query = " UNION ALL ".join(union_queries)
        
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # 先取得總筆數 - 使用相同的查詢邏輯
                count_query = f"SELECT COUNT(*) as total FROM ({full_query}) as combined"
                cur.execute(count_query, params)
                total_records = cur.fetchone()['total']
                
                # Early return - 沒資料就直接返回
                if total_records == 0:
                    return [], 0
                
                # 取得分頁資料 - 在 UNION 外層排序，效能較好
                offset = (page - 1) * page_size
                paginated_query = f"""
                    SELECT total_price, building_total_sqm
                    FROM ({full_query}) as combined
                    WHERE total_price IS NOT NULL 
                      AND building_total_sqm IS NOT NULL
                    ORDER BY total_price DESC
                    LIMIT %s OFFSET %s
                """
                
                cur.execute(paginated_query, params + [page_size, offset])
                results = cur.fetchall()
                
                # 轉換資料格式
                data = []
                for row in results:
                    data.append({
                        'total_price': int(row['total_price']),
                        'total_pin': self._convert_sqm_to_pin(row['building_total_sqm'])
                    })
                
                return data, total_records
    
    def get_used_total_price_and_pin(
        self,
        city_en: str,
        area: Optional[str] = None,
        start_year: int = 113,
        end_year: int = 114,
        page: int = 1,
        page_size: int = 100
    ) -> Tuple[List[Dict], int]:
        """
        查詢中古屋房價總價與總坪數
        
        優化說明:
        1. 單表查詢，不需要 UNION
        2. 只使用 zip_zone (有索引)
        3. WHERE 條件在 NULL 檢查之前，利用索引
        4. 新竹市自動忽略 area 參數 (原始資料限制)
        
        Args:
            city_en: 城市英文代碼
            area: 行政區名稱 (選填) - 對應 zip_zone
            start_year: 起始年份 (民國年)
            end_year: 結束年份 (民國年)
            page: 頁數
            page_size: 每頁筆數
            
        Returns:
            (資料列表, 總筆數)
        """
        # 新竹市不支援區域過濾
        if self._should_ignore_area(city_en):
            area = None
        
        table = f"real_estate_used_{city_en}"
        
        # 構建查詢 - 符合 SARG 原則
        where_conditions = ["transaction_year BETWEEN %s AND %s"]
        params = [start_year, end_year]
        
        if area:
            where_conditions.append("zip_zone = %s")
            params.append(area)
        
        where_clause = " AND ".join(where_conditions)
        
        # 基本查詢
        base_query = f"""
            SELECT 
                total_price,
                building_total_sqm
            FROM {table}
            WHERE {where_clause}
              AND total_price IS NOT NULL
              AND building_total_sqm IS NOT NULL
        """
        
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # 取得總筆數
                count_query = f"SELECT COUNT(*) as total FROM ({base_query}) as subquery"
                cur.execute(count_query, params)
                total_records = cur.fetchone()['total']
                
                # Early return
                if total_records == 0:
                    return [], 0
                
                # 取得分頁資料
                offset = (page - 1) * page_size
                query = f"{base_query} ORDER BY total_price DESC LIMIT %s OFFSET %s"
                cur.execute(query, params + [page_size, offset])
                results = cur.fetchall()
                
                # 轉換資料格式
                data = []
                for row in results:
                    data.append({
                        'total_price': int(row['total_price']),
                        'total_pin': self._convert_sqm_to_pin(row['building_total_sqm'])
                    })
                
                return data, total_records
    
    def get_presale_statistics(
        self,
        city_en: str,
        area: Optional[str] = None,
        start_year: int = 113,
        end_year: int = 114,
        page: int = 1,
        page_size: int = 100
    ) -> Tuple[List[Dict], int]:
        """
        查詢預售屋房價總價與總坪數
        
        優化說明: 同 get_used_total_price_and_pin
        
        Args:
            city_en: 城市英文代碼
            area: 行政區名稱 (選填) - 對應 zip_zone
            start_year: 起始年份 (民國年)
            end_year: 結束年份 (民國年)
            page: 頁數
            page_size: 每頁筆數
            
        Returns:
            (資料列表, 總筆數)
        """
        # 新竹市不支援區域過濾
        if self._should_ignore_area(city_en):
            area = None
        
        table = f"real_estate_presale_{city_en}"
        
        # 構建查詢 - 符合 SARG 原則
        where_conditions = ["transaction_year BETWEEN %s AND %s"]
        params = [start_year, end_year]
        
        if area:
            where_conditions.append("zip_zone = %s")
            params.append(area)
        
        where_clause = " AND ".join(where_conditions)
        
        # 基本查詢
        base_query = f"""
            SELECT 
                total_price,
                building_total_sqm
            FROM {table}
            WHERE {where_clause}
              AND total_price IS NOT NULL
              AND building_total_sqm IS NOT NULL
        """
        
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # 取得總筆數
                count_query = f"SELECT COUNT(*) as total FROM ({base_query}) as subquery"
                cur.execute(count_query, params)
                total_records = cur.fetchone()['total']
                
                # Early return
                if total_records == 0:
                    return [], 0
                
                # 取得分頁資料
                offset = (page - 1) * page_size
                query = f"{base_query} ORDER BY total_price DESC LIMIT %s OFFSET %s"
                cur.execute(query, params + [page_size, offset])
                results = cur.fetchall()
                
                # 轉換資料格式
                data = []
                for row in results:
                    data.append({
                        'total_price': int(row['total_price']),
                        'total_pin': self._convert_sqm_to_pin(row['building_total_sqm'])
                    })
                
                return data, total_records
    
    def get_volume_statistics(
        self,
        city_en: str,
        area: Optional[str] = None,
        start_year: int = 113,
        end_year: int = 114,
        group_by: str = "year",
        building_type: Optional[str] = None
    ) -> Tuple[List[Dict], int]:
        """
        查詢成交量統計
        
        優化說明:
        1. 使用單一查詢完成計數和分組，避免多次掃描
        2. 只選取需要的欄位進行分組
        3. PostgreSQL 會自動優化 COUNT(*) OVER() 的效能
        4. 新竹市自動忽略 area 參數 (原始資料限制)
        
        Args:
            city_en: 城市英文代碼
            area: 行政區名稱 (選填) - 對應 zip_zone
            start_year: 起始年份 (民國年)
            end_year: 結束年份 (民國年)
            group_by: 分組方式 ('year' 或 'quarter')
            building_type: 建物類型 (選填)
            
        Returns:
            (統計資料列表, 總成交量)
        """
        # 新竹市不支援區域過濾
        if self._should_ignore_area(city_en):
            area = None
        
        tables = self._get_table_names(city_en)
        
        # 構建 UNION ALL 查詢 - 只選取需要的欄位
        union_queries = []
        params = []
        
        for table in tables:
            query = f"""
                SELECT 
                    transaction_year,
                    transaction_season
                    {', building_type' if building_type else ''}
                FROM {table}
                WHERE transaction_year BETWEEN %s AND %s
            """
            params.extend([start_year, end_year])
            
            if area:
                query += " AND zip_zone = %s"
                params.append(area)
            
            if building_type:
                query += " AND building_type = %s"
                params.append(building_type)
            
            union_queries.append(query)
        
        # 組合所有查詢
        full_query = " UNION ALL ".join(union_queries)
        
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # 使用單一查詢同時取得總量和分組統計
                # 對於小資料量 (50萬筆)，這比分開查詢更有效率
                if group_by == "quarter":
                    stats_query = f"""
                        SELECT 
                            transaction_year as year,
                            transaction_season as quarter,
                            COUNT(*) as volume,
                            SUM(COUNT(*)) OVER() as total_volume
                        FROM ({full_query}) as combined
                        GROUP BY transaction_year, transaction_season
                        ORDER BY transaction_year, transaction_season
                    """
                else:
                    stats_query = f"""
                        SELECT 
                            transaction_year as year,
                            COUNT(*) as volume,
                            SUM(COUNT(*)) OVER() as total_volume
                        FROM ({full_query}) as combined
                        GROUP BY transaction_year
                        ORDER BY transaction_year
                    """
                
                cur.execute(stats_query, params)
                results = cur.fetchall()
                
                # Early return
                if not results:
                    return [], 0
                
                # 從第一筆資料取得總量 (所有筆的 total_volume 都相同)
                total_volume = int(results[0]['total_volume']) if results else 0
                
                # 轉換資料格式
                data = []
                for row in results:
                    if group_by == "quarter":
                        data.append({
                            'year': int(row['year']),
                            'quarter': int(row['quarter']),
                            'volume': int(row['volume'])
                        })
                    else:
                        data.append({
                            'year': int(row['year']),
                            'volume': int(row['volume'])
                        })
                
                return data, total_volume
    
    def validate_city(self, city_en: str) -> bool:
        """
        驗證城市是否有對應的資料表
        
        優化說明: 使用 EXISTS 而非實際查詢，效能更好
        """
        tables = self._get_table_names(city_en)
        
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # 檢查至少一個表存在
                for table in tables:
                    cur.execute("""
                        SELECT EXISTS (
                            SELECT 1
                            FROM information_schema.tables 
                            WHERE table_schema = 'public' 
                            AND table_name = %s
                        )
                    """, (table,))
                    
                    if cur.fetchone()[0]:
                        return True
                
                return False
