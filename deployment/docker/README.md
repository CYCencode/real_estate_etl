# VM Recovery Guide

## Architecture Overview

Control VM hosts Redis (Celery broker), PostgreSQL (Airflow metadata), Scheduler, Webserver, and API Server. Worker VM executes ETL tasks. Cloud SQL stores processed real estate data.

**Project Structure:**
```
real-estate-etl/
├── deployment/                                    # **IMPORTANT: Dockerfile/requirements.txt changes 
│   ├── docker/                                    #  require manual rebuild via start/stop-services**
│   │   ├── Dockerfile.control                     # Control VM image definition
│   │   ├── Dockerfile.worker                      # Worker VM image definition
│   │   ├── stop-services.sh          # Terminate idle services
│   │   ├── start-services.sh         # Update and start Docker services
│   │   ├── airflow-env.yml                        # Environment variables template
│   │   ├── docker-compose.control.yml             # Control VM Docker Compose config
│   │   ├── docker-compose.worker.yml              # Worker VM Docker Compose config
│   │   ├── vm-init.sh                             # Load secrets from Secret Manager
│   │   └── weekly-maintenance.sh                  # Cron sync DAG/API code & clean log weekly
│   ├──  requirements-control.txt
│   └── requirements-worker.txt
```

**Recovery Summary:**
Stop old services → Create VM from image → Install Docker on new VM → Update Git repo and start Docker → Test services → Configure Cron (optional)

---

## Prerequisites

- GCP Project: `real-estate-202510`
- Region: `us-central1`, Zone: `us-central1-c`
- Machine images: `airflow-control-docker-YYYYMMDD`, `airflow-worker-docker-YYYYMMDD`
- GitHub access and Secret Manager permissions
- FireWall Access setting
- DNS, SSL, Nginx setting
### HTTPS Setup (Control VM Only)

**Install nginx and certbot on Control VM host:**
```bash
sudo apt update
sudo apt install -y nginx certbot python3-certbot-nginx
```

**Configure nginx reverse proxy:**
```bash
sudo nano /etc/nginx/sites-available/api
# Set proxy_pass to http://localhost:8000

sudo ln -s /etc/nginx/sites-available/api /etc/nginx/sites-enabled/
sudo rm /etc/nginx/sites-enabled/default
sudo nginx -t && sudo systemctl reload nginx
```

**Obtain SSL certificate (auto-renewal enabled):**
```bash
sudo certbot --nginx -d api.cyc-encode.site
```

**Verify auto-renewal:**
```bash
sudo systemctl status certbot.timer
sudo certbot renew --dry-run
```
---

## Recovery Steps

### Step 0: Create Backup Images (Routine Maintenance)

Create machine images monthly or after major changes:
- Navigate to GCP Console > Compute Engine > VM instances
- Select VM > Create machine image
- Naming convention: `airflow-control-docker-YYYYMMDD` or `airflow-worker-docker-YYYYMMDD`

---

### Step 1: Create New VMs and Record IPs

**1.1 Terminate Broken VM**

- [ ] **Option A: If VM is accessible**
  ```bash
  gcloud compute ssh [VM_NAME] --zone=us-central1-c
  sudo bash ~/real-estate-etl/deployment/docker/stop-services.sh [control|worker]
  ```

- [ ] **Option B: If VM is inaccessible**
  - Navigate to GCP Console > Compute Engine > VM instances
  - Stop and delete the failed VM

**1.2 Create VM from Machine Image**

```bash
# Control VM
gcloud compute instances create airflow-control-docker \
    --source-machine-image=airflow-control-docker-YYYYMMDD \
    --zone=us-central1-c \
    --project=real-estate-202510

# Worker VM
gcloud compute instances create airflow-worker-docker \
    --source-machine-image=airflow-worker-docker-YYYYMMDD \
    --zone=us-central1-c \
    --project=real-estate-202510
```

- [ ] **Record new Internal IPs:**
  - Control VM Internal IP: `__________________`
  - Worker VM Internal IP: `__________________`

---

### Step 2: Update Configuration Files

Before starting services, update the following files with new Internal IPs and commit to GitHub:

**File: `deployment/docker/airflow-env.yml`**

Update these variables with **Control VM Internal IP**:
```yaml
# Example: Control VM IP = 10.128.0.5
AIRFLOW__CELERY__BROKER_URL=redis://10.128.0.5:6379/0
AIRFLOW__CELERY__RESULT_BACKEND=redis://10.128.0.5:6379/1
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:${AIRFLOW_PG_PASSWORD}@10.128.0.5:5432/airflow_metadata
```

Update this variable with **Worker VM Internal IP**:
```yaml
# Example: Worker VM IP = 10.128.0.6
AIRFLOW__CORE__INTERNAL_API_URL=http://10.128.0.6:8793
```

**File: `deployment/docker/stop-services.sh`**

Update with **Control VM Internal IP**:
```bash
REDIS_HOST="10.128.0.5"  # Update with new Control VM IP
REDIS_PORT="6379"
```

**Commit Changes:**
```bash
git add deployment/docker/airflow-env.yml deployment/docker/stop-services.sh
git commit -m "Update IPs for new VMs - Control: X.X.X.X, Worker: Y.Y.Y.Y"
git push origin main
```

---

### Step 3: Enviroment Setting on New VMs

**3.1 Docker Install**
SSH into each new VM and install Docker:

```bash
# SSH into VM
gcloud compute ssh [airflow-control-docker|airflow-worker-docker] --zone=us-central1-c

# Install Docker
curl -fsSL https://get.docker.com | sudo sh
sudo apt install -y docker-compose-plugin

# Add airflow user to docker group (if needed)
sudo usermod -aG docker airflow
```

- [ ] Control VM: Docker installed
- [ ] Worker VM: Docker installed

**3.2 Update DNS and SSL Certificate (Control VM Only)**
**If Control VM Public IP changed, update DNS and renew certificate:**
```bash
# 1. Get new Control VM Public IP
gcloud compute instances describe airflow-control-docker \
  --zone=us-central1-c \
  --format='get(networkInterfaces[0].accessConfigs[0].natIP)'

# 2. Update GoDaddy DNS A Record
# - Login to GoDaddy DNS Management
# - Update A Record: api.cyc-encode.site → [New Public IP]
# - TTL: 600

# 3. Verify DNS propagation
dig api.cyc-encode.site +short
# Should return new IP

# 4. Test nginx and renew SSL certificate
curl http://localhost:8000/health
sudo certbot renew --force-renewal

# 5. Verify HTTPS
curl https://api.cyc-encode.site/health
```

- [ ] Control VM: DNS updated and SSL certificate renewed
---

### Step 4: Deploy Services

**4.1 Update Git Repository**

```bash
# Switch to airflow user
sudo su - airflow

# Pull latest code with updated IPs
cd ~/real-estate-etl
git fetch origin main
git reset --hard origin/main
```

**4.2 Start Docker Services**

```bash
cd deployment/docker

# Load secrets from Secret Manager
source vm-init.sh

# Start services
bash start-services.sh [control|worker]
```

- [ ] Control VM: Services started
- [ ] Worker VM: Services started

---

### Step 5: Verify Services

#### Control VM Verification

```bash
# ==================== 1. Container Status ====================
echo "===== Docker Container Status ====="
sudo docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# Expected output:
# airflow-scheduler    Up X minutes    Healthy
# airflow-webserver    Up X minutes    Healthy (0.0.0.0:8080->8080/tcp)
# airflow-redis        Up X minutes    Healthy (0.0.0.0:6379->6379/tcp)
# api-server           Up X minutes    Healthy (0.0.0.0:8000->8000/tcp)

# ==================== 2. Service Health Check ====================
echo -e "\n===== Scheduler Health ====="
sudo docker exec airflow-scheduler airflow jobs check --job-type SchedulerJob --hostname "$(docker exec airflow-scheduler hostname)" 2>/dev/null && echo "✓ Scheduler OK" || echo "⚠ Scheduler FAIL"

echo -e "\n===== Webserver Health ====="
curl -s http://localhost:8080/health | grep -q "healthy" && echo "✓ Webserver OK" || echo "⚠ Webserver FAIL"

echo -e "\n===== Redis Connection ====="
sudo docker exec airflow-redis redis-cli ping && echo "✓ Redis OK"

echo -e "\n===== API Server Health ====="
curl -s http://localhost:8000/health && echo ""

# ==================== 3. DAG Loading Status ====================
echo -e "\n===== DAG Import Errors ====="
sudo docker exec airflow-scheduler airflow dags list-import-errors

echo -e "\n===== Available DAGs ====="
sudo docker exec airflow-scheduler airflow dags list | grep real_estate

# ==================== 4. Celery Worker Registration ====================
echo -e "\n===== Celery Worker Status ====="
sudo docker exec airflow-scheduler celery -A airflow.executors.celery_executor.app inspect active

# Expected output:
# -> celery@airflow-worker-docker: OK

# ==================== 5. Log Check ====================
echo -e "\n===== Scheduler Recent Logs ====="
sudo docker logs airflow-scheduler --tail 30 | grep -E "(Scheduler started|Processing|DAG)"

echo -e "\n===== API Server Recent Logs ====="
sudo docker logs api-server --tail 20

# ==================== 6. System Resources ====================
echo -e "\n===== System Resource Usage ====="
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"
```

**Control VM Checklist:**
- [ ] All 4 containers running (scheduler, webserver, redis, api-server)
- [ ] Scheduler job check passed
- [ ] Webserver health endpoint returns healthy
- [ ] Redis responds to ping
- [ ] API server health endpoint accessible
- [ ] No DAG import errors
- [ ] Real estate DAGs listed
- [ ] Celery worker registered

#### Worker VM Verification

```bash
# ==================== 1. Container Status ====================
echo "===== Docker Container Status ====="
sudo docker ps --format "table {{.Names}}\t{{.Status}}"

# Expected output:
# airflow-worker    Up X minutes    Healthy

# ==================== 2. Worker Logs ====================
echo -e "\n===== Worker Recent Logs ====="
sudo docker logs airflow-worker --tail 50 | grep -E "(Connected|ready|concurrency)"

# ==================== 3. Celery Connection Status ====================
echo -e "\n===== Celery Worker Self-Check ====="
sudo docker exec airflow-worker celery -A airflow.executors.celery_executor.app inspect ping

# Expected output:
# -> celery@airflow-worker-docker: OK
#     pong

# ==================== 4. Worker System Resources ====================
echo -e "\n===== Worker Resource Usage ====="
docker stats --no-stream airflow-worker
```

**Worker VM Checklist:**
- [ ] Worker container running and healthy
- [ ] Worker logs show successful connection to Redis
- [ ] Celery ping responds with pong
- [ ] Resource usage is normal

---

### Step 6: Configure Cron Job (Optional)

Set up automatic code synchronization and service restart:

```bash
# Edit crontab for airflow user
sudo crontab -u airflow -e

# Add weekly auto-update (every Saturday at 10:00 AM)
0 10 * * 6 bash /home/airflow/real-estate-etl/deployment/docker/weekly-maintenance.sh control >> /var/log/airflow-maintenance.log 2>&1

# or
0 10 * * 6 bash /home/airflow/real-estate-etl/deployment/docker/weekly-maintenance.sh worker >> /var/log/airflow-maintenance.log 2>&1

```

- [ ] Cron job configured

---

## Troubleshooting

**Issue: Worker cannot connect to Redis**
- Verify Control VM Internal IP in `airflow-env.yml`
- Test connection: `redis-cli -h [CONTROL_VM_IP] -p 6379 ping`
- Check firewall rules allow port 6379 between VMs

**Issue: DAG import errors**
- Verify Python dependencies: Rebuild Docker image if `requirements.txt` changed
- Check DAG folder mount: `docker exec airflow-scheduler ls /opt/airflow/dags`

**Issue: Secret Manager access denied**
- Verify VM service account has `roles/secretmanager.secretAccessor`
- Re-run: `source ~/real-estate-etl/deployment/docker/vm-init.sh`

**Issue: Database connection failed**
- Verify Control VM Internal IP in `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`
- Test PostgreSQL container: `docker exec airflow-postgres psql -U airflow -c '\l'`

---

## Important Notes

- Cloud SQL has built-in redundancy; only VM recovery needed
- Machine images include all files, users, and installed packages
- Always update configuration files before starting services
- Code should be updated via `git pull` after VM recovery
- Internal IPs may change; update all configuration files accordingly
