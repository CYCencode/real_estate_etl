-- ====================================
-- 預售屋交易資料表 Schema (以新竹市為例)
-- ====================================

CREATE TABLE IF NOT EXISTS real_estate_presale_hsinchucity (
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
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION
);

-- 建立索引
CREATE INDEX IF NOT EXISTS idx_real_estate_presale_hsinchucity_zip_zone 
    ON real_estate_presale_hsinchucity(zip_zone);

CREATE INDEX IF NOT EXISTS idx_real_estate_presale_hsinchucity_transaction_year 
    ON real_estate_presale_hsinchucity(transaction_year);

CREATE INDEX IF NOT EXISTS idx_real_estate_presale_hsinchucity_transaction_season 
    ON real_estate_presale_hsinchucity(transaction_season);

-- Schema 說明
COMMENT ON TABLE real_estate_presale_hsinchucity IS '新竹市預售屋交易資料';
COMMENT ON COLUMN real_estate_presale_hsinchucity.id IS '自動遞增主鍵';
COMMENT ON COLUMN real_estate_presale_hsinchucity.land_total_sqm IS '土地總面積(平方公尺)';
COMMENT ON COLUMN real_estate_presale_hsinchucity.building_total_sqm IS '建物總面積(平方公尺)';
COMMENT ON COLUMN real_estate_presale_hsinchucity.room_count IS '房間數';
COMMENT ON COLUMN real_estate_presale_hsinchucity.use_zone IS '使用分區';
COMMENT ON COLUMN real_estate_presale_hsinchucity.zip_zone IS '郵遞區號';
COMMENT ON COLUMN real_estate_presale_hsinchucity.unit_price_per_sqm IS '單價(元/平方公尺)';
COMMENT ON COLUMN real_estate_presale_hsinchucity.total_price IS '總價(元)';
COMMENT ON COLUMN real_estate_presale_hsinchucity.address IS '地址';
COMMENT ON COLUMN real_estate_presale_hsinchucity.locate_floor IS '所在樓層';
COMMENT ON COLUMN real_estate_presale_hsinchucity.total_floor IS '總樓層數';
COMMENT ON COLUMN real_estate_presale_hsinchucity.building_type IS '建物類型';
COMMENT ON COLUMN real_estate_presale_hsinchucity.transaction_year IS '交易年份';
COMMENT ON COLUMN real_estate_presale_hsinchucity.transaction_season IS '交易季度(1-4)';
COMMENT ON COLUMN real_estate_presale_hsinchucity.latitude IS '緯度';
COMMENT ON COLUMN real_estate_presale_hsinchucity.longitude IS '經度';