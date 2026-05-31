#!/usr/bin/env bash
# ============================================================
# run_local.sh
# Run Slope One in local mode on a single machine.
# Useful for testing before deploying to the cluster.
#
# Requirements:
#   pip install pyspark matplotlib seaborn numpy pandas
# ============================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

echo ""
echo "======================================================"
echo "  DS5001 Cluster-09 — Slope One (Local Mode)"
echo "======================================================"
echo ""

# Install dependencies if needed
python3 -c "import pyspark" 2>/dev/null || {
    echo "Installing PySpark ..."
    pip3 install pyspark==3.5.1 matplotlib seaborn numpy pandas --break-system-packages -q
}

# Verify data
if [ ! -f "$SCRIPT_DIR/data/ratings.dat" ] && [ ! -f "$SCRIPT_DIR/data/ratings.csv" ]; then
    echo "Dataset not found. Downloading ..."
    bash "$SCRIPT_DIR/scripts/download_data.sh"
fi

python3 "$SCRIPT_DIR/src/slope_one.py" \
    --mode local \
    --data-dir "$SCRIPT_DIR/data" \
    --results-dir "$SCRIPT_DIR/results" \
    --top-n 10 \
    --skip-scalability

echo ""
echo "Results saved to results/"
