#!/bin/bash
# ============================================
# deployment/docker/stop-services.sh
# 用途：完全終止所有 Airflow 相關服務（systemd + Docker + 進程）
# 執行時機：在啟動 Docker 服務之前，確保沒有衝突
# 執行者：root 使用者（需要 sudo 權限）
# 執行：sudo bash stop-services.sh [control|worker]
# ============================================

set -e

# 確認以 root 執行（因為需要 systemctl 和 kill 權限）
if [ "$EUID" -ne 0 ]; then
    echo "ERROR: This script must be run as root"
    echo "Use: sudo bash stop-services.sh [control|worker]"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODE=${1:-control}  # control 或 worker

echo "========================================"
echo "Terminating All Airflow Services"
echo "Mode: $MODE"
echo "Hostname: $(hostname)"
echo "Date: $(date)"
echo "========================================"
echo ""

# ============================================
# 1. 停止並禁用 systemd 服務
# ============================================
echo "[1] Stopping systemd services..."
echo "----------------------------------------"

SYSTEMD_SERVICES=("airflow-scheduler" "airflow-webserver" "airflow-worker")

for service in "${SYSTEMD_SERVICES[@]}"; do
    if systemctl list-unit-files | grep -q "^${service}.service"; then
        echo "Stopping and disabling: ${service}"
        systemctl stop "$service" 2>/dev/null || true
        systemctl disable "$service" 2>/dev/null || true
    fi
done

echo "✓ Systemd services stopped"
echo ""

# ============================================
# 2. 停止 Docker 容器
# ============================================
echo "[2] Stopping Docker containers..."
echo "----------------------------------------"

cd "$SCRIPT_DIR"

if [ "$MODE" = "control" ]; then
    if [ -f "docker-compose.control.yml" ]; then
        echo "Stopping Control VM containers..."
        docker compose -f docker-compose.control.yml down 2>/dev/null || true
    fi
elif [ "$MODE" = "worker" ]; then
    if [ -f "docker-compose.worker.yml" ]; then
        echo "Stopping Worker VM containers..."
        docker compose -f docker-compose.worker.yml down 2>/dev/null || true
    fi
fi

echo "✓ Docker containers stopped"
echo ""

# ============================================
# 3. 強制終止所有 airflow 進程
# ============================================
echo "[3] Killing airflow processes..."
echo "----------------------------------------"

AIRFLOW_PIDS=$(ps aux | grep -E "[a]irflow" | awk '{print $2}')
if [ -n "$AIRFLOW_PIDS" ]; then
    echo "Found airflow processes: $AIRFLOW_PIDS"
    kill -9 $AIRFLOW_PIDS 2>/dev/null || true
    sleep 2
    echo "✓ Airflow processes killed"
else
    echo "✓ No airflow processes found"
fi
echo ""

# ============================================
# 4. 清理 Redis 狀態（僅 Control VM）
# ============================================
if [ "$MODE" = "control" ]; then
    echo "[4] Cleaning Redis state..."
    echo "----------------------------------------"
    
    REDIS_HOST="10.128.0.4"
    REDIS_PORT="6379"
    
    # 檢查 Redis 是否可連線
    if command -v redis-cli &> /dev/null; then
        if redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" ping &> /dev/null; then
            echo "Flushing Celery state from Redis..."
            redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" --scan --pattern "celery*" | \
                xargs -r redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" DEL
            echo "✓ Redis cleaned"
        else
            echo "⚠ Redis not accessible (will be cleaned when Redis starts)"
        fi
    else
        echo "ℹ redis-cli not installed, skipping Redis cleanup"
    fi
    echo ""
fi

# ============================================
# 5. 清理 Docker 系統（可選）
# ============================================
echo "[5] Cleaning Docker system (optional)..."
echo "----------------------------------------"

read -p "Clean Docker system? (dangling images, stopped containers) [y/N]: " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    docker system prune -f
    echo "✓ Docker system cleaned"
else
    echo "Skipped"
fi
echo ""

# ============================================
# 6. 驗證清理結果
# ============================================
echo "[6] Verification..."
echo "----------------------------------------"

REMAINING_PROCS=$(ps aux | grep -E "[a]irflow" || true)
if [ -n "$REMAINING_PROCS" ]; then
    echo "⚠ WARNING: Some airflow processes still running:"
    echo "$REMAINING_PROCS"
else
    echo "✓ No airflow processes running"
fi

RUNNING_CONTAINERS=$(docker ps --filter "name=airflow" --format "{{.Names}}" 2>/dev/null || true)
if [ -n "$RUNNING_CONTAINERS" ]; then
    echo "⚠ WARNING: Some Docker containers still running:"
    echo "$RUNNING_CONTAINERS"
else
    echo "✓ No Docker containers running"
fi

echo ""
echo "========================================"
echo "Termination Complete!"
echo "========================================"
echo ""
echo "Next steps:"
echo "  1. Verify no conflicts: bash diagnose-conflicts.sh"
echo "  2. Start Docker services: sudo -u airflow bash step2_start_docker_services.sh $MODE"
echo ""