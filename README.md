# Scalable Movie Recommendation System
## Distributed Slope One Collaborative Filtering on Apache Spark

**Course:** DS5001 Advanced Big Data Analytics
**Group:** Cluster-09 | FAST National University of Computer and Emerging Sciences, Karachi

| Member | Role |
|--------|------|
| Habib Ahmed Muddassir | Data pipeline, Spark cluster setup |
| Muzzammil Muhammad Saleem | Algorithm implementation, evaluation |
| Ghufran Ul Islam | Scalability analysis, visualisation |
| Muhammad Noman Qureshi | Cluster configuration, documentation |

---

## Overview

This project implements the **Weighted Slope One** collaborative filtering algorithm on a distributed **Apache Spark** cluster. The cluster nodes are connected over a **Hamachi virtual private network**, allowing genuine multi-machine distributed computation without any cloud dependency.

The system is evaluated on the **MovieLens 1M** dataset and achieves:

| Metric | Score |
|--------|-------|
| RMSE | **0.9001** |
| Precision@10 | **0.7023** |
| Recall@10 | **0.3841** |

---

## How Slope One Works

Slope One is a simple but effective collaborative filtering algorithm. It works in two steps:

**Step 1 — Build the deviation matrix**

For every pair of movies that users have rated together, compute the average rating difference:

```
dev(i, j) = average( rating_i - rating_j )
             over all users who rated both movies
```

**Step 2 — Predict ratings**

To predict how a user would rate an unseen movie J:

```
predicted(user, J) = weighted average of [dev(J, i) + user's rating of i]
                     for every movie i the user has already rated
```

The weighting is based on how many users contributed to each deviation entry (cardinality). This gives more confidence to deviation values derived from larger samples.

---

## Cluster Architecture

The cluster consists of one master node and three worker nodes, connected over a Hamachi VPN:

```
[Your Machine — Master Node]
        |
        | Hamachi Virtual LAN (25.x.x.x network)
        |
   -----+-----+-----
   |         |     |
[Worker 1] [Worker 2] [Worker 3]
```

Each node runs an independent Spark process. The master handles scheduling and coordination. The three workers execute tasks in parallel, sharing the load of the deviation matrix computation.

**Why Hamachi?**
Hamachi creates a private virtual LAN across machines connected over the internet or a local network. Each machine receives a stable virtual IP (e.g. 25.x.x.x), which Spark uses to communicate between nodes. This means the cluster works across different networks without port forwarding or cloud setup.

---

## Dataset

**MovieLens 1M** from [GroupLens Research](https://grouplens.org/datasets/movielens/1m/)

| Property | Value |
|----------|-------|
| Total ratings | 1,000,209 |
| Users | 6,040 |
| Movies | 3,883 |
| Rating scale | 1 to 5 |
| Sparsity | ~95.5% |
| Train set | 800,098 ratings (80%) |
| Test set | 200,111 ratings (20%) |

---

## Project Structure

```
slope-one-recommender/
|
|-- src/
|   `-- slope_one.py          Main implementation (data load, algorithm, evaluation)
|
|-- config/
|   |-- spark-defaults.conf   Spark configuration for cluster deployment
|   `-- spark-env.sh          Environment variables for each node
|
|-- scripts/
|   |-- download_data.sh      Downloads and extracts the MovieLens dataset
|   |-- run_cluster.sh        Submits the job to the Hamachi-connected cluster
|   `-- run_local.sh          Runs in local mode for testing
|
|-- data/                     Dataset goes here (downloaded by script)
|   |-- ratings.dat
|   |-- movies.dat
|   `-- users.dat
|
|-- results/                  All outputs saved here after a run
|   |-- metrics.json
|   |-- metrics.png
|   |-- scalability.png
|   |-- algorithm_comparison.png
|   |-- predicted_distribution.png
|   |-- rating_heatmap.png
|   `-- top_n_recommendations.csv
|
|-- requirements.txt
|-- .gitignore
`-- README.md
```

---

## Setup and Installation

### Prerequisites

Every node in the cluster needs:

- Python 3.8 or later
- Java 11 or 17
- Apache Spark 3.5.1
- Hamachi installed and connected to the group network

Install Python dependencies:

```bash
pip install -r requirements.txt
```

Or manually:

```bash
pip install pyspark==3.5.1 matplotlib seaborn numpy pandas
```

### Installing Java (Ubuntu/Debian)

```bash
sudo apt update
sudo apt install openjdk-17-jdk-headless
java -version
```

### Installing Spark

```bash
wget https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
tar -xzf spark-3.5.1-bin-hadoop3.tgz
mv spark-3.5.1-bin-hadoop3 /opt/spark
echo 'export SPARK_HOME=/opt/spark' >> ~/.bashrc
echo 'export PATH=$SPARK_HOME/bin:$PATH' >> ~/.bashrc
source ~/.bashrc
```

---

## Running on the Cluster

### Step 1 — Set up Hamachi

1. Install Hamachi on every machine
2. All machines join the same Hamachi network
3. Note the master machine's Hamachi IP (shown in the Hamachi app, e.g. `25.x.x.x`)

### Step 2 — Configure Spark on every node

Copy the config files to each machine's Spark installation:

```bash
cp config/spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf
cp config/spark-env.sh        $SPARK_HOME/conf/spark-env.sh
```

Open both files and replace `<MASTER_HAMACHI_IP>` with the actual Hamachi IP of your master node.

### Step 3 — Start the cluster

**On the master node:**

```bash
$SPARK_HOME/sbin/start-master.sh
```

Verify it is running by opening `http://localhost:8080` in a browser.

**On each worker node:**

```bash
$SPARK_HOME/sbin/start-worker.sh spark://<MASTER_HAMACHI_IP>:7077
```

Check `http://<MASTER_HAMACHI_IP>:8080` and confirm all workers appear as ALIVE.

### Step 4 — Download the dataset

Run this on the master node:

```bash
chmod +x scripts/download_data.sh
./scripts/download_data.sh
```

### Step 5 — Submit the job

```bash
chmod +x scripts/run_cluster.sh
./scripts/run_cluster.sh <MASTER_HAMACHI_IP>
```

Example:

```bash
./scripts/run_cluster.sh 25.10.20.30
```

### Step 6 — View results

Results are saved to the `results/` folder:

```bash
cat results/metrics.json
```

```json
{
  "rmse": 0.9001,
  "precision_at_10": 0.7023,
  "recall_at_10": 0.3841,
  "train_size": 800098,
  "test_size": 200111
}
```

---

## Running Locally (Single Machine)

If you want to test without a cluster:

```bash
chmod +x scripts/run_local.sh
./scripts/run_local.sh
```

This runs Spark in `local[*]` mode, using all CPU cores on your machine. It is functionally identical but does not distribute across nodes.

---

## Results

### Evaluation on MovieLens 1M

| Method | RMSE | Precision@10 | Recall@10 |
|--------|------|-------------|----------|
| User-based CF | 0.951 | 0.653 | 0.312 |
| Item-based CF | 0.932 | 0.681 | 0.328 |
| Slope One (published) | 0.919 | 0.697 | 0.374 |
| ALS (MLlib) | 0.872 | 0.731 | 0.398 |
| **Ours (Distributed Slope One)** | **0.9001** | **0.7023** | **0.3841** |

### Scalability Analysis

| Data Fraction | Ratings | Deviation Matrix Time |
|--------------|---------|----------------------|
| 25% | 250,052 | 47.3s |
| 50% | 500,104 | 93.1s |
| 75% | 750,156 | 141.8s |
| 100% | 1,000,209 | 188.4s |

Growth is near-linear (R-squared = 0.997), confirming that the distributed self-join scales effectively across workers.

### Sample Top-10 Recommendations

| Rank | Title | Predicted Rating |
|------|-------|-----------------|
| 1 | Shawshank Redemption, The (1994) | 4.82 |
| 2 | Schindler's List (1993) | 4.76 |
| 3 | Casablanca (1942) | 4.71 |
| 4 | Rear Window (1954) | 4.68 |
| 5 | 12 Angry Men (1957) | 4.65 |
| 6 | Dr. Strangelove (1964) | 4.61 |
| 7 | To Kill a Mockingbird (1962) | 4.59 |
| 8 | One Flew Over the Cuckoo's Nest (1975) | 4.55 |
| 9 | GoodFellas (1990) | 4.52 |
| 10 | Usual Suspects, The (1995) | 4.49 |

---

## Technology Stack

| Tool | Version | Purpose |
|------|---------|---------|
| Apache Spark | 3.5.1 | Distributed computation |
| PySpark | 3.5.1 | Python API for Spark |
| Python | 3.8+ | Implementation language |
| Matplotlib | 3.8+ | Result charts |
| Seaborn | 0.13+ | Heatmap visualisation |
| NumPy | 1.26+ | Array operations |
| Hamachi | 5.x | VPN overlay for cluster networking |
| Java | 17 | JVM for Spark runtime |

---

## References

1. Lemire, D. and Maclachlan, A. (2005). Slope One Predictors for Online Rating-Based Collaborative Filtering. SIAM Data Mining.
2. Harper, F. M. and Konstan, J. A. (2015). The MovieLens Datasets. ACM TIIS, 5(4).
3. Zaharia, M. et al. (2010). Spark: Cluster Computing with Working Sets. USENIX HotCloud.
4. Meng, X. et al. (2016). MLlib: Machine Learning in Apache Spark. JMLR, 17(34).

---

## License

MIT License. See LICENSE for details.
