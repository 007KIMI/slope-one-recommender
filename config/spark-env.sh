#!/usr/bin/env bash
# ============================================================
# spark-env.sh — Spark Environment Configuration
# ============================================================
# Copy to $SPARK_HOME/conf/spark-env.sh on every node.
# ============================================================

# Java home (update path if needed)
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Python for PySpark
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3

# Master settings (set MASTER node only)
export SPARK_MASTER_HOST=<MASTER_HAMACHI_IP>
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080

# Worker settings (set on WORKER nodes)
# Adjust memory to leave enough for the OS and other processes
export SPARK_WORKER_MEMORY=4g
export SPARK_WORKER_CORES=2
export SPARK_WORKER_WEBUI_PORT=8081
