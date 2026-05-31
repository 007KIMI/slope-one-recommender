#!/usr/bin/env bash
# ============================================================
# download_data.sh
# Downloads and extracts the MovieLens 1M dataset.
# Run this on the master node before submitting the job.
# ============================================================

set -e

DATA_DIR="$(dirname "$0")/../data"
mkdir -p "$DATA_DIR"

if [ -f "$DATA_DIR/ratings.dat" ]; then
    echo "Dataset already present in $DATA_DIR — skipping download."
    ls -lh "$DATA_DIR"
    exit 0
fi

echo "Downloading MovieLens 1M (~25 MB) ..."
wget -q --show-progress \
    "https://files.grouplens.org/datasets/movielens/ml-1m.zip" \
    -O "$DATA_DIR/ml-1m.zip"

echo "Extracting ..."
python3 -c "
import zipfile, os
z = zipfile.ZipFile('$DATA_DIR/ml-1m.zip')
z.extractall('$DATA_DIR/')
"

mv "$DATA_DIR/ml-1m/ratings.dat" "$DATA_DIR/"
mv "$DATA_DIR/ml-1m/movies.dat"  "$DATA_DIR/"
mv "$DATA_DIR/ml-1m/users.dat"   "$DATA_DIR/" 2>/dev/null || true
rm -rf "$DATA_DIR/ml-1m" "$DATA_DIR/ml-1m.zip"

echo ""
echo "Dataset ready:"
ls -lh "$DATA_DIR"
