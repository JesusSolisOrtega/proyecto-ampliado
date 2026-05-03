#!/bin/bash
# Pipeline Big Data — Arranque completo
export MSYS_NO_PATHCONV=1

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
log()  { echo -e "${GREEN}[$(date +%H:%M:%S)]${NC} $1"; }
warn() { echo -e "${YELLOW}[$(date +%H:%M:%S)]${NC} $1"; }
err()  { echo -e "${RED}[$(date +%H:%M:%S)]${NC} $1"; exit 1; }

echo ""
echo "================================================="
echo "   Pipeline Big Data — Consumo Electrico Endesa  "
echo "================================================="
echo ""

# ── 1. Levantar cluster ───────────────────────────────
log "[1/4] Levantando cluster Docker..."
docker compose up -d || err "Fallo al levantar Docker. ¿Está Docker Desktop abierto?"

# ── 2. Esperar Kafka ──────────────────────────────────
log "[2/4] Esperando a que Kafka esté listo..."
RETRIES=0
until docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 > /dev/null 2>&1; do
  RETRIES=$((RETRIES + 1))
  if [ $RETRIES -ge 12 ]; then
    err "Kafka no respondió tras 60s. Revisa: docker compose ps"
  fi
  warn "  Kafka no listo aún, reintentando en 5s... ($RETRIES/12)"
  sleep 5
done
log "Kafka operativo."

# ── 3. Crear topic ────────────────────────────────────
log "[3/4] Creando topic 'consumo_streaming'..."
docker exec kafka kafka-topics \
  --create --if-not-exists \
  --topic consumo_streaming \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1 > /dev/null 2>&1
log "Topic listo."

# ── 4. Lanzar Spark Streaming Job en background ───────
log "[4/4] Lanzando Spark Structured Streaming Job..."
mkdir -p logs
nohup docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \
  /app/spark_streaming_job.py > logs/spark_job.log 2>&1 &

log "Spark Job arrancado en background. Log: logs/spark_job.log"
log "Esperando 15s para que Spark inicialice..."
sleep 15

echo ""
log "=== ¡Pipeline listo! ==="
echo ""
echo "  Lanza el productor en esta terminal:"
echo ""
echo "    source .venv/Scripts/activate   # Windows Git Bash"
echo "    python productor.py"
echo ""
echo "  Paneles de monitorización:"
echo "    Kafka-UI  ->  http://localhost:8082"
echo "    Spark UI  ->  http://localhost:8080"
echo "    Zeppelin  ->  http://localhost:8888"
echo ""
echo "  Para parar todo: bash stop.sh"
echo ""
