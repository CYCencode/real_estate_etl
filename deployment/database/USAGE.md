# Database Usage Guide
## Quick Deployment

### Step 1: Create Configuration Tables (Required for Initial Deployment)
```bash
# Connect to PostgreSQL and execute initialization scripts
psql -h <host> -p 5432 -U postgres -d postgres \
  -f deployment/database/init/01_create_config_tables.sql

psql -h <host> -p 5432 -U postgres -d postgres \
  -f deployment/database/init/02_insert_initial_data.sql
```

### Step 2: Automatic Table Creation
Presale and used real estate tables will be automatically created when DAGs run for the first time. No manual operation required.

---

## Email Configuration Management

### 1. Recipients Management (email_recipients)

#### Add a Recipient
```sql
INSERT INTO email_recipients (email, is_enabled, created_by, notes) VALUES
    ('user@example.com', 1, 'admin', 'Finance department recipient');
```

#### Disable a Recipient (Soft Deletion)
```sql
UPDATE email_recipients 
SET is_enabled = 0, 
    update_time = CURRENT_TIMESTAMP 
WHERE email = 'olduser@example.com';
```
---

### 2. Sender Management (email_sender)

#### Add a Sender

```sql
INSERT INTO email_sender (email, is_admin, created_by, notes) VALUES
    ('alerts@yourdomain.com', 0, 'admin', 'Alert notifications only');
```

#### Set Admin Sender
```sql
INSERT INTO email_sender (email, is_admin, created_by, notes) VALUES
    ('admin@yourdomain.com', 1, 'system', 'Administrator sender')
ON CONFLICT (email) DO NOTHING;
```

#### Change Admin Permission
```sql
UPDATE email_sender 
SET is_admin = 1,
    update_time = CURRENT_TIMESTAMP
WHERE email = 'newadmin@yourdomain.com';
```

---

## Schema Reference

For detailed table structures, please refer to the schemas/ directory:

- schemas/email_recipients.sql - Recipient table structure
- schemas/email_sender.sql - Sender table structure
- schemas/real_estate_presale.sql - Presale real estate table structure
- schemas/real_estate_used.sql - Used real estate table structure
