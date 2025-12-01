-- ====================================
-- 建立 Email 配置表
-- 執行: psql -h <host> -U postgres -d postgres -f 01_create_config_tables.sql
-- ====================================

\echo '開始建立 Email 配置表...'

-- 建立 email_recipients 表
CREATE TABLE IF NOT EXISTS email_recipients (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    created_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    is_enabled SMALLINT DEFAULT 1 CHECK (is_enabled IN (0, 1)),
    created_by VARCHAR(100),
    notes TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS email_recipients_email_key 
    ON email_recipients USING btree (email);

CREATE INDEX IF NOT EXISTS idx_email_recipients_enabled 
    ON email_recipients USING btree (is_enabled);

\echo '✓ email_recipients 表建立完成'

-- 建立 email_sender 表
CREATE TABLE IF NOT EXISTS email_sender (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    created_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    is_admin SMALLINT DEFAULT 0 CHECK (is_admin IN (0, 1)),
    created_by VARCHAR(100),
    notes TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS email_sender_email_key 
    ON email_sender USING btree (email);

CREATE INDEX IF NOT EXISTS idx_email_sender_admin 
    ON email_sender USING btree (is_admin);

\echo '✓ email_sender 表建立完成'
\echo '所有配置表建立完成!'