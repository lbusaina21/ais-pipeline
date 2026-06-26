#!/bin/bash
# init_pipeline.sh
# Dijalankan otomatis oleh Onyxia saat service launch.
# Urutan: clone repo → install deps → jalankan pipeline → tulis sentinel → self-destruct

set -euo pipefail

REPO_URL="https://github.com/lbusaina21/ais-pipeline.git"
BRANCH="main"
WORK_DIR="/home/onyxia/work"
ONYXIA_API="https://datalab.officialstatistics.org"
LOG_FILE="/home/onyxia/work/pipeline.log"

# Ambil info dari environment variable yang di-inject Onyxia
WORKING_DIR="${AWS_WORKING_DIRECTORY_PATH}"
SERVICE_ID="${ONYXIA_SERVICE_ID:-}"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

write_sentinel() {
    local status=$1
    local message=$2
    local key="${WORKING_DIR}pipeline_status/${status}"

    echo "$message" | aws s3 cp - "s3://${key}" \
        --endpoint-url "${AWS_S3_ENDPOINT:-}" 2>/dev/null || \
    python3 -c "
import boto3, os
s3 = boto3.client('s3')
working_dir = os.environ['AWS_WORKING_DIRECTORY_PATH']
bucket = working_dir.split('/')[0]
prefix = '/'.join(working_dir.split('/')[1:])
s3.put_object(
    Bucket=bucket,
    Key=f'{prefix}pipeline_status/${status}',
    Body='${message}'.encode()
)
print('Sentinel ${status} ditulis ke S3.')
"
}

self_destruct() {
    if [ -n "$SERVICE_ID" ]; then
        log "Self-destruct: menghapus service $SERVICE_ID..."
        curl -s -X DELETE \
            "${ONYXIA_API}/my-services/${SERVICE_ID}" \
            -H "Authorization: Bearer ${VAULT_TOKEN}" || true
    else
        log "SERVICE_ID tidak tersedia, skip self-destruct."
    fi
}

# ── Trap error — tulis sentinel ERROR lalu self-destruct ─────────────────────
trap 'log "ERROR di baris $LINENO"; write_sentinel "ERROR" "Gagal di baris $LINENO. Lihat log: $LOG_FILE"; self_destruct' ERR

log "=========================================="
log "Pipeline AIS Indonesia dimulai"
log "=========================================="

# ── 1. Clone repo ─────────────────────────────────────────────────────────────
log "Clone repo dari GitLab..."
if [ -d "$WORK_DIR" ]; then
    cd "$WORK_DIR" && git pull origin "$BRANCH"
else
    git clone --branch "$BRANCH" "$REPO_URL" "$WORK_DIR"
fi
cd "$WORK_DIR"
log "Repo siap di $WORK_DIR"

# ── 2. Install dependencies ───────────────────────────────────────────────────
log "Install dependencies..."
pip install -q --no-deps \
    "git+ssh://git@code.officialstatistics.org/trade-task-team-phase-1/ais.git@emr-migration"
pip install h3==3.7.7
pip install -q rapidfuzz h3==3.7.7 geodatasets pyodbc sqlalchemy
pip install --upgrade --no-cache-dir ./ais_aoi_integrated
log "Dependencies selesai."

# ── 3. Jalankan pipeline ──────────────────────────────────────────────────────

# Konfigurasi akumulatif
export PIPELINE_ACCUM_START="okt2025"

log "--- Mulai: 00_ingest.py ---"
python pipeline/00_ingest.py 2>&1 | tee -a "$LOG_FILE"
log "--- Selesai: 00_ingest.py ---"

log "--- Mulai: 01_preprocess.py ---"
python pipeline/01_preprocess.py 2>&1 | tee -a "$LOG_FILE"
log "--- Selesai: 01_preprocess.py ---"

log "--- Mulai analisis paralel ---"
python pipeline/02_port_traffic.py 2>&1 | tee -a "$LOG_FILE" &
PID_TRAFFIC=$!
python pipeline/03_port_call.py 2>&1 | tee -a "$LOG_FILE" &
PID_CALL=$!
python pipeline/04_time_travel.py 2>&1 | tee -a "$LOG_FILE" &
PID_TIME=$!

wait $PID_TRAFFIC || { log "02_port_traffic.py GAGAL"; exit 1; }
wait $PID_CALL    || { log "03_port_call.py GAGAL"; exit 1; }
wait $PID_TIME    || { log "04_time_travel.py GAGAL"; exit 1; }
log "--- Selesai: semua analisis paralel ---"

log "--- Mulai: 05_load_sqlserver.py ---"
python pipeline/05_load_sqlserver.py 2>&1 | tee -a "$LOG_FILE"
log "--- Selesai: 05_load_sqlserver.py ---"

# ── 4. Tulis sentinel SUCCESS ─────────────────────────────────────────────────
FINISH_TIME=$(date '+%Y-%m-%d %H:%M:%S')
write_sentinel "SUCCESS" "Pipeline selesai pada $FINISH_TIME"
log "Sentinel SUCCESS ditulis ke S3."

# ── 5. Self-destruct ──────────────────────────────────────────────────────────
log "Pipeline selesai. Self-destruct service..."
self_destruct

log "Done."
