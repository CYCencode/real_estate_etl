-- ====================================
-- 插入初始資料範例
-- 執行: psql -h <host> -U postgres -d postgres -f 02_insert_initial_data.sql
-- ====================================

\echo '開始插入初始資料...'

-- 插入 email_sender 範例資料
INSERT INTO email_sender (email, is_admin, created_by, notes) VALUES
    ('noreply@yourdomain.com', 1, 'system', '系統預設發件者')
ON CONFLICT (email) DO NOTHING;

\echo '✓ email_sender 初始資料插入完成'

-- 插入 email_recipients 範例資料
INSERT INTO email_recipients (email, is_enabled, created_by, notes) VALUES
    ('admin@yourdomain.com', 1, 'system', '管理員信箱'),
    ('report@yourdomain.com', 1, 'system', '報告收件信箱')
ON CONFLICT (email) DO NOTHING;

\echo '✓ email_recipients 初始資料插入完成'
\echo '所有初始資料插入完成!'