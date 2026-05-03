#!/bin/bash
# Pipeline Big Data — Parada limpia
export MSYS_NO_PATHCONV=1

GREEN='\033[0;32m'; NC='\033[0m'
log() { echo -e "${GREEN}[$(date +%H:%M:%S)]${NC} $1"; }

echo ""
log "Parando Spark Streaming Job (si está corriendo)..."
docker exec spark-master pkill -f spark_streaming_job.py 2>/dev/null || true

log "Parando cluster Docker..."
docker compose stop

echo ""
log "Cluster parado. Los datos Parquet se conservan en data/parquet_output/"
echo ""
