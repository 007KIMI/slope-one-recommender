# Hamachi Cluster Setup Guide

This guide walks through connecting multiple machines into a Spark cluster
using Hamachi as the networking layer.

---

## What is Hamachi?

Hamachi is a VPN tool that creates a private virtual LAN between any machines
over the internet. Each machine gets a stable virtual IP address (25.x.x.x),
which Spark uses to communicate between nodes. This means you can run a real
distributed cluster across machines on different networks without any router
configuration or cloud services.

---

## Step 1 — Install Hamachi on Every Machine

**On Ubuntu/Linux:**
```bash
wget https://www.vpn.net/installers/logmein-hamachi_2.1.0.203-1_amd64.deb
sudo dpkg -i logmein-hamachi_2.1.0.203-1_amd64.deb
sudo hamachi login
sudo hamachi create <network-name> <password>   # master only
sudo hamachi join <network-name> <password>     # workers
```

**On Windows:**
Download and install from: https://vpn.net

---

## Step 2 — Find Your Hamachi IP

After joining the network, your Hamachi IP appears in the app (e.g. 25.x.x.x).

On Linux you can also run:
```bash
sudo hamachi
```

Note the master machine's IP. Workers need this to connect to Spark.

---

## Step 3 — Verify Connectivity

From each worker, ping the master:
```bash
ping <MASTER_HAMACHI_IP>
```

All nodes should be reachable before starting Spark.

---

## Step 4 — Configure Spark

On every node, update the config files:

```bash
cp config/spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf
cp config/spark-env.sh        $SPARK_HOME/conf/spark-env.sh
```

Replace all occurrences of `<MASTER_HAMACHI_IP>` with the real IP.

---

## Step 5 — Start the Cluster

**Master node:**
```bash
$SPARK_HOME/sbin/start-master.sh
```

**Each worker node:**
```bash
$SPARK_HOME/sbin/start-worker.sh spark://<MASTER_HAMACHI_IP>:7077
```

---

## Step 6 — Verify in the Spark Web UI

Open a browser on any machine and go to:
```
http://<MASTER_HAMACHI_IP>:8080
```

You should see all worker nodes listed as ALIVE with their cores and memory.

---

## Stopping the Cluster

**On the master:**
```bash
$SPARK_HOME/sbin/stop-all.sh
```
