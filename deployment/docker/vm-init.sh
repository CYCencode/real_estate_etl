#!/bin/bash
# ==========================================
# VM Bootstrap Script
# 用途：在 VM 啟動時載入 Secret Manager 並啟動 Docker
# 執行時機：VM 重建後首次啟動，或手動重啟服務
# ==========================================

set -e

echo "[INFO] Starting Airflow Docker bootstrap..."

PROJECT_ID="real-estate-202510"

# ==========================================
# 從 Secret Manager 載入機敏資訊到 VM 環境
# ==========================================
echo "[INFO] Loading secrets from Secret Manager..."

export PG_PASSWORD=$(gcloud secrets versions access latest --secret=PG_PASSWORD --project=${PROJECT_ID})
export AIRFLOW_PG_PASSWORD=$(gcloud secrets versions access latest --secret=AIRFLOW_PG_PASSWORD --project=${PROJECT_ID})
export FERNET_KEY=$(gcloud secrets versions access latest --secret=AIRFLOW_FERNET_KEY --project=${PROJECT_ID})
export WEBSERVER_SECRET_KEY=$(gcloud secrets versions access latest --secret=AIRFLOW_WEBSERVER_SECRET_KEY --project=${PROJECT_ID})
export SLACK_WEBHOOK_URL=$(gcloud secrets versions access latest --secret=SLACK_WEBHOOK_URL --project=${PROJECT_ID})
export HERE_API_KEY=$(gcloud secrets versions access latest --secret=HERE_API_KEY --project=${PROJECT_ID})
export GMAIL_APP_PASSWORD=$(gcloud secrets versions access latest --secret=GMAIL_APP_PASSWORD --project=${PROJECT_ID})
export GOOGLE_MAPS_API_KEY=$(gcloud secrets versions access latest --secret=GOOGLE_MAPS_API_KEY --project=${PROJECT_ID})

echo "[INFO] Secrets loaded successfully"