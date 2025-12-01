#!/bin/bash
# ============================================
# deployment/docker/start-services.sh
# 用途：啟動 Docker Airflow 服務
# 執行時機：確認舊服務已終止後
# 執行者：airflow 使用者
# 執行：sudo bash start-services.sh [control|worker]
# ============================================

set -e

# 確認以 airflow 使用者執行
if [ "$(whoami)" != "airflow" ]; then
    echo "ERROR: This script must be run as 'airflow' user"
    echo "Use: sudo -u airflow bash start-services.sh [control|worker]"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
MODE=${1:-control}  # control 或 worker

echo "========================================"
echo "Starting Airflow Docker Services"
echo "Mode: $MODE"
echo "Hostname: $(hostname)"
echo "Date: $(date)"
echo "========================================"
echo ""

# ============================================
# 1. 預檢查
# ============================================
echo "[1] Pre-flight checks..."
echo "----------------------------------------"

# 檢查 Docker 是否運行
if ! docker info &> /dev/null; then
    echo "❌ Docker is not running!"
    exit 1
fi
echo "✓ Docker is running"

# 檢查 compose 檔案
if [ "$MODE" = "control" ]; then
    COMPOSE_FILE="docker-compose.control.yml"
elif [ "$MODE" = "worker" ]; then
    COMPOSE_FILE="docker-compose.worker.yml"
else
    echo "❌ Invalid mode: $MODE (must be 'control' or 'worker')"
    exit 1
fi

if [ ! -f "$SCRIPT_DIR/$COMPOSE_FILE" ]; then
    echo "❌ Compose file not found: $COMPOSE_FILE"
    exit 1
fi
echo "✓ Compose file found: $COMPOSE_FILE"

# 檢查 vm-init.sh
if [ ! -f "$SCRIPT_DIR/vm-init.sh" ]; then
    echo "❌ vm-init.sh not found!"
    echo "   This file is required to load secrets from Secret Manager"
    exit 1
fi
echo "✓ vm-init.sh found"

# 檢查是否有衝突的服務
AIRFLOW_PROCS=$(ps aux | grep -E "[a]irflow" || true)
if [ -n "$AIRFLOW_PROCS" ]; then
    echo "⚠ WARNING: Airflow processes detected!"
    echo "$AIRFLOW_PROCS"
    echo ""
    read -p "Continue anyway? [y/N]: " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Cancelled. Please run: sudo bash step1_terminate_current_services.sh $MODE"
        exit 1
    fi
fi

echo ""

# ============================================
# 2. 載入環境變數
# ============================================
echo "[2] Loading environment variables..."
echo "----------------------------------------"

echo "Loading secrets from GCP Secret Manager..."
source "$SCRIPT_DIR/vm-init.sh"

# 驗證關鍵變數
REQUIRED_VARS=(
    "PG_PASSWORD"
    "AIRFLOW_PG_PASSWORD"
    "FERNET_KEY"
    "SLACK_WEBHOOK_URL"
)

MISSING_VARS=false
for var in "${REQUIRED_VARS[@]}"; do
    if [ -z "${!var}" ]; then
        echo "❌ Missing: $var"
        MISSING_VARS=true
    else
        echo "✓ Loaded: $var"
    fi
done

if [ "$MISSING_VARS" = true ]; then
    echo "❌ Some required variables are missing!"
    exit 1
fi

echo "✓ All required variables loaded"
echo ""

# ============================================
# 3. 更新程式碼
# ============================================
echo "[3] Code update ..."
echo "----------------------------------------"

read -p "Pull latest code from GitHub? [y/N]: " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    cd "$PROJECT_ROOT"
    echo "Fetching latest code..."
    git fetch origin main
    git reset --hard origin/main
    echo "✓ Code updated"
else
    echo "Skipped"
fi
echo ""

# ============================================
# 3.5. 檢查是否需要重建映像檔
# ============================================
echo "[3.5] Docker image build check..."
echo "----------------------------------------"

DOCKERFILE_PATH="$SCRIPT_DIR/Dockerfile.${MODE}"
if [ -f "$DOCKERFILE_PATH" ]; then
    if [ "$MODE" = "control" ]; then
        IMAGE_NAME="real-estate-airflow-control:latest"
    else
        IMAGE_NAME="real-estate-airflow-worker:latest"
    fi
    
    IMAGE_EXISTS=$(docker images -q "$IMAGE_NAME" 2>/dev/null)
    
    if [ -z "$IMAGE_EXISTS" ]; then
        echo "⚠️ Docker image not found, will build"
        NEED_BUILD=true
    else
        echo "✓ Found existing image: $IMAGE_NAME"
        read -p "Rebuild Docker images? (Required after Dockerfile/requirements changes) [y/N]: " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            NEED_BUILD=true
        else
            NEED_BUILD=false
        fi
    fi
else
    echo "⚠️ Dockerfile not found: $DOCKERFILE_PATH"
    NEED_BUILD=false
fi

echo ""

# ============================================
# 4. 啟動 Docker 服務
# ============================================
echo "[4] Starting Docker services..."
echo "----------------------------------------"

cd "$SCRIPT_DIR"

# 如果需要重建映像檔
if [ "$NEED_BUILD" = true ]; then
    echo "Cleaning space before build..."
    # 清理所有未使用的 images
    docker image prune -a -f

    echo "Building Docker images (this may take 5-10 minutes)..."
    # 根據 MODE 建置正確的映像檔
    if [ "$MODE" = "control" ]; then
        docker build -t real-estate-airflow-control:latest -f Dockerfile.control ../..
    else
        docker build -t real-estate-airflow-worker:latest -f Dockerfile.worker ../..
    fi
    
    echo "✓ Images built"
fi

echo "Starting services from $COMPOSE_FILE..."
docker compose -f "$COMPOSE_FILE" up -d --no-build

echo "✓ Services started"
echo ""

# ============================================
# 5. 等待服務健康檢查
# ============================================
echo "[5] Waiting for services to be healthy..."
echo "----------------------------------------"

echo "Waiting 30 seconds for initialization..."
sleep 30

echo ""
echo "Service status:"
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
echo ""

# ============================================
# 6. 驗證服務
# ============================================
echo "[6] Service verification..."
echo "----------------------------------------"

if [ "$MODE" = "control" ]; then
    # 檢查 scheduler
    if docker ps | grep -q "airflow-scheduler"; then
        echo "✓ Scheduler is running"
        
        # 檢查 DAG 載入
        echo ""
        echo "Checking DAG import errors..."
        docker exec airflow-scheduler airflow dags list-import-errors 2>/dev/null || echo "No import errors (or container not ready)"
        
        # 列出 DAG
        echo ""
        echo "Available DAGs:"
        docker exec airflow-scheduler airflow dags list 2>/dev/null | grep -E "(dag_id|real_estate)" || echo "DAGs not loaded yet"
    else
        echo "⚠ Scheduler not running"
    fi
    
    # 檢查 webserver
    if docker ps | grep -q "airflow-webserver"; then
        echo "✓ Webserver is running"
        echo "   Access UI: http://$(hostname -I | awk '{print $1}'):8080"
    else
        echo "⚠ Webserver not running"
    fi
    
    # 檢查 Redis
    if docker ps | grep -q "airflow-redis"; then
        echo "✓ Redis is running"
    else
        echo "⚠ Redis not running"
    fi
    
    # 檢查 API server
    if docker ps | grep -q "api-server"; then
        echo "✓ API server is running"
        echo "   Health check: curl http://localhost:8000/health"
    else
        echo "⚠ API server not running"
    fi

elif [ "$MODE" = "worker" ]; then
    # 檢查 worker
    if docker ps | grep -q "airflow-worker"; then
        echo "✓ Worker is running"
        
        # 檢查 Celery 註冊
        echo ""
        echo "Checking Celery worker registration..."
        sleep 10  # 等待 worker 完全啟動
        docker exec airflow-worker celery -A airflow.executors.celery_executor.app inspect active 2>/dev/null || echo "Worker not registered yet"
    else
        echo "⚠ Worker not running"
    fi
fi

echo ""

# ============================================
# 7. 顯示日誌
# ============================================
echo "[7] Recent logs..."
echo "----------------------------------------"

if [ "$MODE" = "control" ]; then
    echo "Scheduler logs (last 20 lines):"
    docker logs airflow-scheduler --tail 20 2>/dev/null || echo "No logs available"
elif [ "$MODE" = "worker" ]; then
    echo "Worker logs (last 20 lines):"
    docker logs airflow-worker --tail 20 2>/dev/null || echo "No logs available"
fi

echo ""
echo "========================================"
echo "Startup Complete!"
echo "========================================"
echo ""
echo "Next steps:"
if [ "$MODE" = "control" ]; then
    echo "  1. Check UI: http://$(hostname -I | awk '{print $1}'):8080"
    echo "  2. View scheduler logs: docker logs -f airflow-scheduler"
    echo "  3. Test DAG: docker exec airflow-scheduler airflow dags trigger real_estate_quarterly_etl"
    echo "  4. Check API: curl http://localhost:8000/health"
elif [ "$MODE" = "worker" ]; then
    echo "  1. View worker logs: docker logs -f airflow-worker"
    echo "  2. Check worker status:"
    echo "     docker exec airflow-worker celery -A airflow.executors.celery_executor.app inspect active"
fi
echo ""