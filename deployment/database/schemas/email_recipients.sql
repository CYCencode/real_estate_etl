-- ====================================
-- Email 收件者配置表 Schema
-- ====================================

CREATE TABLE IF NOT EXISTS email_recipients (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    created_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    is_enabled SMALLINT DEFAULT 1 CHECK (is_enabled IN (0, 1)),
    created_by VARCHAR(100),
    notes TEXT
);

-- 建立索引
CREATE UNIQUE INDEX IF NOT EXISTS email_recipients_pkey 
    ON email_recipients USING btree (id);

CREATE UNIQUE INDEX IF NOT EXISTS email_recipients_email_key 
    ON email_recipients USING btree (email);

CREATE INDEX IF NOT EXISTS idx_email_recipients_enabled 
    ON email_recipients USING btree (is_enabled);

-- Schema 說明
COMMENT ON TABLE email_recipients IS 'Email 報告收件者清單';
COMMENT ON COLUMN email_recipients.id IS '自動遞增主鍵';
COMMENT ON COLUMN email_recipients.email IS '收件者 Email 地址(唯一)';
COMMENT ON COLUMN email_recipients.created_time IS '建立時間';
COMMENT ON COLUMN email_recipients.update_time IS '最後更新時間';
COMMENT ON COLUMN email_recipients.is_enabled IS '是否啟用(0:停用, 1:啟用)';
COMMENT ON COLUMN email_recipients.created_by IS '建立者';
COMMENT ON COLUMN email_recipients.notes IS '備註說明';