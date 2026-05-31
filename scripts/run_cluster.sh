#!/usr/bin/env bash
# ============================================================
# run_cluster.sh
# Submit the Slope One job to the Spark cluster.
#
# Usage:
#   ./scripts/run_cluster.sh <MASTER_HAMACHI_IP>
#
# Example:
#   ./scripts/run_cluster.sh 25.10.20.30
# ============================================================

set -e

MASTER_IP=${1:-""}

if [ -z "$MASTER_IP" ]; then
    echo "ERROR: Please provide the master node Hamachi IP."
    echo "Usage: ./scripts/run_cluster.sh <MASTER_HAMACHI_IP>"
    echo ""
    echo "To find your Hamachi IP, open the Hamachi app."
    echo "It shows as a number like 25.x.x.x next to your machine name."
    exit 1
fi

MASTER_URL="spark://${MASTER_IP}:7077"
SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

echo ""
echo "======================================================"
echo "  DS5001 Cluster-09 — Slope One on Spark"
echo "  Master: $MASTER_URL"
echo "======================================================"
echo ""

# Verify data is present
if [ ! -f "$SCRIPT_DIR/data/ratings.dat" ] && [ ! -f "$SCRIPT_DIR/data/ratings.csv" ]; then
    echo "ERROR: Dataset not found. Run scripts/download_data.sh first."
    exit 1
fi

spark-submit \
    --master "$MASTER_URL" \
    --executor-memory 3g \
    --executor-cores 2 \
    --num-executors 3 \
    --driver-memory 2g \
    --conf spark.sql.shuffle.partitions=50 \
    --conf spark.network.timeout=600s \
    --conf spark.executor.heartbeatInterval=60s \
    "$SCRIPT_DIR/src/slope_one.py" \
        --mode cluster \
        --master "$MASTER_URL" \
        --data-dir "$SCRIPT_DIR/data" \
        --results-dir "$SCRIPT_DIR/results" \
        --top-n 10

echo ""
echo "======================================================"
echo "  Job complete. Results saved to results/"
echo "======================================================"
