#!/bin/bash
# ============================================
# deployment/docker/weekly-maintenance.sh
# 用途：每週例行維護（同步程式碼、清理日誌、更新服務）
# 執行時機：每週六 10:00（由 crontab 觸發）
# 執行者：airflow 使用者
# 執行：bash weekly-maintenance.sh [control|worker]
# ============================================

set -e

# 確認以 airflow 使用者執行
if [ "$(whoami)" != "airflow" ]; then
    echo "ERROR: This script must be run as 'airflow' user"
    echo "Use: sudo -u airflow bash weekly-maintenance.sh [control|worker]"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
MODE=${1:-control}  # control 或 worker

LOG_PREFIX="[Weekly Maintenance $(date '+%Y-%m-%d %H:%M:%S')]"

echo "$LOG_PREFIX Starting weekly maintenance for $MODE VM..."

# ============================================
# 1. 檢查 Git 是否有更新
# ============================================
cd "$PROJECT_ROOT"

git fetch origin main
LOCAL=$(git rev-parse HEAD)
REMOTE=$(git rev-parse origin/main)

HAS_UPDATE=false

if [ "$LOCAL" = "$REMOTE" ]; then
    echo "$LOG_PREFIX No updates found, skipping restart"
else
    echo "$LOG_PREFIX Updates found, proceeding with update..."
    echo "$LOG_PREFIX Local:  $LOCAL"
    echo "$LOG_PREFIX Remote: $REMOTE"
    HAS_UPDATE=true
    
    # ============================================
    # 2. 更新程式碼
    # ============================================
    cd "$PROJECT_ROOT"
    git reset --hard origin/main
    
    # 清理未追蹤的檔案，但排除 logs/ 目錄（避免權限問題）
    git clean -fd -e logs/ -e '*.log' 2>/dev/null || true
    
    echo "$LOG_PREFIX Code updated to $REMOTE"
    
    # ============================================
    # 3. 重啟 Docker 服務
    # ============================================
    cd "$SCRIPT_DIR"
    
    echo "$LOG_PREFIX Restarting Docker services..."
    
    # 載入環境變數
    if [ -f "$SCRIPT_DIR/vm-init.sh" ]; then
        source "$SCRIPT_DIR/vm-init.sh"
    else
        echo "$LOG_PREFIX ERROR: vm-init.sh not found!"
        exit 1
    fi
    
    # 重啟服務
    if [ "$MODE" = "control" ]; then
        docker compose -f docker-compose.control.yml down
        docker compose -f docker-compose.control.yml up -d
    elif [ "$MODE" = "worker" ]; then
        docker compose -f docker-compose.worker.yml down
        docker compose -f docker-compose.worker.yml up -d
    else
        echo "$LOG_PREFIX ERROR: Invalid mode '$MODE'. Use 'control' or 'worker'"
        exit 1
    fi
    
    echo "$LOG_PREFIX Docker services restarted"
    
    # ============================================
    # 4. 等待服務啟動
    # ============================================
    echo "$LOG_PREFIX Waiting 30s for services to start..."
    sleep 30
    
    # ============================================
    # 5. 驗證服務狀態
    # ============================================
    FAILED_CONTAINERS=$(docker ps -a --filter "status=exited" --format "{{.Names}}" | grep airflow || true)
    
    if [ -n "$FAILED_CONTAINERS" ]; then
        echo "$LOG_PREFIX WARNING: Some containers failed to start:"
        echo "$FAILED_CONTAINERS"
        
        # 發送告警（如果有設定 Slack）
        if [ -n "$SLACK_WEBHOOK_URL" ]; then
            curl -X POST "$SLACK_WEBHOOK_URL" \
                 -H 'Content-Type: application/json' \
                 -d "{\"text\":\"⚠ Airflow maintenance failed on $MODE VM\\nFailed containers: $FAILED_CONTAINERS\"}"
        fi
        
        exit 1
    fi
    
    echo "$LOG_PREFIX All containers running successfully"
fi

# ============================================
# 6. 清理任務（無論是否有更新都執行）
# ============================================
echo "$LOG_PREFIX Running cleanup tasks..."

# 清理 Docker 資源
docker system prune -f --filter "until=336h"
docker container prune -f
docker volume prune -f

echo "$LOG_PREFIX Docker resources cleaned"

# 清理日誌 volume
if [ "$MODE" = "control" ]; then
    LOG_SIZE=$(docker run --rm -v airflow-logs:/logs alpine:latest du -sh /logs | awk '{print $1}')
    echo "$LOG_PREFIX Control VM log size before cleanup: $LOG_SIZE"
    
    docker run --rm -v airflow-logs:/logs alpine:latest \
        sh -c "find /logs -type f -mtime +30 -delete && find /logs -type d -empty -delete"
    
    LOG_SIZE_AFTER=$(docker run --rm -v airflow-logs:/logs alpine:latest du -sh /logs | awk '{print $1}')
    echo "$LOG_PREFIX Control VM log size after cleanup: $LOG_SIZE_AFTER"
    
elif [ "$MODE" = "worker" ]; then
    LOG_SIZE=$(docker run --rm -v worker-logs:/logs alpine:latest du -sh /logs | awk '{print $1}')
    echo "$LOG_PREFIX Worker VM log size before cleanup: $LOG_SIZE"
    
    docker run --rm -v worker-logs:/logs alpine:latest \
        sh -c "find /logs -type f -mtime +30 -delete && find /logs -type d -empty -delete"
    
    LOG_SIZE_AFTER=$(docker run --rm -v worker-logs:/logs alpine:latest du -sh /logs | awk '{print $1}')
    echo "$LOG_PREFIX Worker VM log size after cleanup: $LOG_SIZE_AFTER"
fi

echo "$LOG_PREFIX Cleanup completed"

# ============================================
# 7. 發送成功通知
# ============================================
if [ -n "$SLACK_WEBHOOK_URL" ]; then
    if [ "$HAS_UPDATE" = true ]; then
        MESSAGE="✅ Airflow weekly maintenance completed on $MODE VM\\nUpdated to: $REMOTE\\nLog volume: $LOG_SIZE → $LOG_SIZE_AFTER"
    else
        MESSAGE="✅ Airflow weekly cleanup completed on $MODE VM\\nNo updates found\\nLog volume: $LOG_SIZE → $LOG_SIZE_AFTER"
    fi
    
    curl -X POST "$SLACK_WEBHOOK_URL" \
         -H 'Content-Type: application/json' \
         -d "{\"text\":\"$MESSAGE\"}"
fi

echo "$LOG_PREFIX Weekly maintenance completed successfully"
exit 0