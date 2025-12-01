-- ====================================
-- Email 發件者配置表 Schema
-- ====================================

CREATE TABLE IF NOT EXISTS email_sender (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    created_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    is_admin SMALLINT DEFAULT 0 CHECK (is_admin IN (0, 1)),
    created_by VARCHAR(100),
    notes TEXT
);

-- 建立索引
CREATE UNIQUE INDEX IF NOT EXISTS email_sender_pkey 
    ON email_sender USING btree (id);

CREATE UNIQUE INDEX IF NOT EXISTS email_sender_email_key 
    ON email_sender USING btree (email);

CREATE INDEX IF NOT EXISTS idx_email_sender_admin 
    ON email_sender USING btree (is_admin);

-- Schema 說明
COMMENT ON TABLE email_sender IS 'Email 發件者配置清單';
COMMENT ON COLUMN email_sender.id IS '自動遞增主鍵';
COMMENT ON COLUMN email_sender.email IS '發件者 Email 地址(唯一)';
COMMENT ON COLUMN email_sender.created_time IS '建立時間';
COMMENT ON COLUMN email_sender.update_time IS '最後更新時間';
COMMENT ON COLUMN email_sender.is_admin IS '是否為管理員(0:否, 1:是)';
COMMENT ON COLUMN email_sender.created_by IS '建立者';
COMMENT ON COLUMN email_sender.notes IS '備註說明';