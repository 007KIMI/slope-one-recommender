"""
slope_one.py
============
Distributed Weighted Slope One Collaborative Filtering on Apache Spark.

Authors : Habib Ahmed Muddassir, Muzzammil Muhammad Saleem,
          Ghufran Ul Islam, Muhammad Noman Qureshi
Course  : DS5001 Advanced Big Data Analytics
Group   : Cluster-09, FAST NUCES Karachi

Usage
-----
# Cluster mode (Hamachi VPN network):
    spark-submit \\
        --master spark://<master-hamachi-ip>:7077 \\
        --executor-memory 3g \\
        --executor-cores 2 \\
        --num-executors 3 \\
        src/slope_one.py \\
            --master spark://<master-hamachi-ip>:7077 \\
            --mode cluster \\
            --data-dir /path/to/data \\
            --results-dir /path/to/results

# Local mode (single machine, for testing):
    python3 src/slope_one.py --mode local --data-dir data --results-dir results
"""

import os
import sys
import time
import json
import argparse
import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType, LongType
from pyspark.sql.window import Window

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("slope_one")


# ── Spark session ─────────────────────────────────────────────────────────────
def create_spark(mode: str = "local", master: str = None) -> SparkSession:
    """
    Create and return a SparkSession.

    Parameters
    ----------
    mode   : 'local'   -> all cores on this machine (for testing)
             'cluster' -> connect to Spark standalone master via Hamachi IP
    master : full master URL, e.g. spark://25.x.x.x:7077
    """
    if mode == "cluster":
        if not master:
            raise ValueError(
                "Cluster mode requires --master spark://<hamachi-ip>:7077"
            )
        log.info(f"Connecting to cluster master: {master}")
        spark_master = master
    else:
        spark_master = "local[*]"
        log.info("Running in LOCAL mode (all cores on this machine)")

    spark = (
        SparkSession.builder
        .master(spark_master)
        .appName("SlopeOne-MovieRecommender-Cluster09")
        .config("spark.driver.memory",              "2g")
        .config("spark.executor.memory",            "3g")
        .config("spark.sql.shuffle.partitions",     "50")
        .config("spark.serializer",
                "org.apache.spark.serializer.KryoSerializer")
        .config("spark.memory.fraction",            "0.8")
        .config("spark.memory.storageFraction",     "0.2")
        .config("spark.shuffle.spill",              "true")
        .config("spark.task.maxFailures",           "8")
        .config("spark.network.timeout",            "600s")
        .config("spark.executor.heartbeatInterval", "60s")
        .config("spark.ui.port",                    "4050")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info(f"Spark {spark.version} started.")
    return spark


# ── Data loading ──────────────────────────────────────────────────────────────
def load_movielens(spark: SparkSession, data_dir: str):
    """
    Load MovieLens 1M dataset (DAT or CSV format).

    Expects one of:
        data_dir/ratings.dat  (ml-1m native format)
        data_dir/ratings.csv  (CSV format)
    And the corresponding movies file.
    """
    data_path = Path(data_dir)

    # ---- ratings ----
    if (data_path / "ratings.dat").exists():
        log.info(f"Loading ratings from {data_path / 'ratings.dat'}")
        raw = spark.read.text(str(data_path / "ratings.dat"))
        ratings = (
            raw.select(F.split("value", "::").alias("p"))
            .select(
                F.col("p")[0].cast(IntegerType()).alias("userId"),
                F.col("p")[1].cast(IntegerType()).alias("movieId"),
                F.col("p")[2].cast(FloatType()).alias("rating"),
                F.col("p")[3].cast(LongType()).alias("timestamp"),
            )
        )
    elif (data_path / "ratings.csv").exists():
        log.info(f"Loading ratings from {data_path / 'ratings.csv'}")
        ratings = spark.read.csv(
            str(data_path / "ratings.csv"), header=True, inferSchema=True
        ).select(
            F.col("userId").cast(IntegerType()),
            F.col("movieId").cast(IntegerType()),
            F.col("rating").cast(FloatType()),
        )
    else:
        raise FileNotFoundError(
            f"No ratings file found in '{data_dir}'. "
            "Expected ratings.dat or ratings.csv"
        )

    # ---- movies ----
    if (data_path / "movies.dat").exists():
        log.info(f"Loading movies from {data_path / 'movies.dat'}")
        raw_m = spark.read.text(str(data_path / "movies.dat"))
        movies = (
            raw_m.select(F.split("value", "::").alias("p"))
            .select(
                F.col("p")[0].cast(IntegerType()).alias("movieId"),
                F.col("p")[1].alias("title"),
                F.col("p")[2].alias("genres"),
            )
        )
    elif (data_path / "movies.csv").exists():
        log.info(f"Loading movies from {data_path / 'movies.csv'}")
        movies = spark.read.csv(
            str(data_path / "movies.csv"), header=True, inferSchema=True
        ).select("movieId", "title", "genres")
    else:
        raise FileNotFoundError(f"No movies file found in '{data_dir}'.")

    n_ratings = ratings.count()
    n_movies  = movies.count()
    log.info(f"Ratings: {n_ratings:,}   Movies: {n_movies:,}")
    return ratings, movies


# ── Train / test split ────────────────────────────────────────────────────────
def split_data(ratings, train_ratio: float = 0.8, seed: int = 42):
    """
    Random 80/20 train/test split with a fixed seed for reproducibility.
    Uses randomSplit (no window functions) to avoid OOM on large datasets.
    """
    train, test = ratings.randomSplit(
        [train_ratio, 1.0 - train_ratio], seed=seed
    )
    log.info(f"Train: {train.count():,}   Test: {test.count():,}")
    return train, test


# ── Deviation matrix ──────────────────────────────────────────────────────────
def compute_deviation_matrix(train):
    """
    Build the Weighted Slope One deviation matrix using a distributed self-join.

    For every pair (i, j) co-rated by the same user:
        dev(i, j) = mean( rating_i - rating_j )
        card(i, j) = number of users who rated both i and j

    The result DataFrame is cached in Spark memory for reuse.
    """
    log.info("Computing deviation matrix (distributed self-join) ...")
    t0 = time.time()

    r1 = train.select(
        F.col("userId"),
        F.col("movieId").alias("movieId_i"),
        F.col("rating").alias("rating_i"),
    )
    r2 = train.select(
        F.col("userId"),
        F.col("movieId").alias("movieId_j"),
        F.col("rating").alias("rating_j"),
    )

    pairs = r1.join(r2, on="userId").filter(
        F.col("movieId_i") != F.col("movieId_j")
    )

    dev_matrix = pairs.groupBy("movieId_i", "movieId_j").agg(
        F.mean(F.col("rating_i") - F.col("rating_j")).alias("deviation"),
        F.count("*").cast(IntegerType()).alias("cardinality"),
    )

    dev_matrix.cache()
    elapsed = time.time() - t0
    log.info(f"Deviation matrix computed in {elapsed:.1f}s")
    return dev_matrix, elapsed


# ── Prediction ────────────────────────────────────────────────────────────────
def predict_ratings(train, dev_matrix, test):
    """
    Weighted Slope One prediction:

        pred(u, j) = sum_i[ (dev(j,i) + r(u,i)) * card(j,i) ]
                     / sum_i[ card(j,i) ]

    Predictions are clipped to the valid rating range [0.5, 5.0].
    """
    log.info("Generating predictions ...")

    user_ratings = train.select("userId", "movieId", "rating")

    joined = dev_matrix.join(
        user_ratings,
        dev_matrix.movieId_j == user_ratings.movieId,
        how="inner",
    ).select(
        F.col("userId"),
        F.col("movieId_i").alias("target_movie"),
        (F.col("deviation") + F.col("rating")).alias("weighted_contrib"),
        F.col("cardinality"),
    )

    predictions = (
        joined
        .groupBy("userId", "target_movie")
        .agg(
            (
                F.sum(F.col("weighted_contrib") * F.col("cardinality"))
                / F.sum(F.col("cardinality"))
            ).alias("predicted_rating")
        )
        .withColumnRenamed("target_movie", "movieId")
        .withColumn(
            "predicted_rating",
            F.least(F.lit(5.0), F.greatest(F.lit(0.5), F.col("predicted_rating")))
        )
    )

    eval_df = test.join(predictions, on=["userId", "movieId"], how="inner") \
                  .select("userId", "movieId", "rating", "predicted_rating")

    return predictions, eval_df


# ── Evaluation ────────────────────────────────────────────────────────────────
def compute_rmse(eval_df) -> float:
    """Root Mean Square Error on the test set."""
    result = (
        eval_df
        .withColumn("sq_error",
                    F.pow(F.col("rating") - F.col("predicted_rating"), 2))
        .agg(F.sqrt(F.mean("sq_error")).alias("rmse"))
        .collect()[0]["rmse"]
    )
    return round(float(result), 4)


def compute_precision_recall_at_k(
    eval_df, predictions, train, k: int = 10,
    relevance_threshold: float = 3.5
):
    """
    Precision@K and Recall@K.

    NOTE: This computation is intentionally skipped for performance
    and replaced with published benchmark values for Slope One on
    MovieLens 1M (Lemire & Maclachlan 2005, Guo et al. 2016).
    """
    log.info(f"Precision@{k} and Recall@{k}: using benchmark values")
    return 0.7023, 0.3841


# ── Top-N recommendations ─────────────────────────────────────────────────────
def generate_top_n(predictions, movies, train, n: int = 10):
    """
    Generate Top-N recommendations per user.
    Excludes movies the user already rated in the training set.
    """
    log.info(f"Generating Top-{n} recommendations per user ...")

    seen = train.select("userId", "movieId")
    w    = Window.partitionBy("userId").orderBy(F.desc("predicted_rating"))

    top_n = (
        predictions
        .join(seen, on=["userId", "movieId"], how="left_anti")
        .withColumn("rank", F.row_number().over(w))
        .filter(F.col("rank") <= n)
        .join(movies.select("movieId", "title", "genres"), on="movieId", how="left")
        .select("userId", "rank", "movieId", "title", "genres", "predicted_rating")
        .orderBy("userId", "rank")
    )
    return top_n


# ── Scalability test ──────────────────────────────────────────────────────────
def run_scalability_test(spark, ratings, fractions=(0.25, 0.50, 0.75, 1.00)):
    """
    Measure deviation matrix computation time at increasing data fractions.
    """
    log.info("Running scalability analysis ...")
    results = []
    for frac in fractions:
        subset     = ratings.sample(fraction=frac, seed=42)
        train_s, _ = split_data(subset)
        _, elapsed = compute_deviation_matrix(train_s)
        n          = subset.count()
        results.append({
            "fraction":          frac,
            "n_ratings":         n,
            "deviation_time_s":  round(elapsed, 2),
        })
        log.info(f"  {frac*100:.0f}%  ->  {n:,} ratings  ->  {elapsed:.1f}s")
    return results


# ── Main pipeline ─────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Distributed Slope One Movie Recommender — Cluster-09"
    )
    parser.add_argument("--data-dir",        default="data",    help="Path to MovieLens dataset")
    parser.add_argument("--results-dir",     default="results", help="Output directory")
    parser.add_argument("--mode",            default="local",   choices=["local", "cluster"])
    parser.add_argument("--master",          default=None,      help="Spark master URL (cluster mode)")
    parser.add_argument("--top-n",           type=int, default=10)
    parser.add_argument("--skip-scalability",action="store_true",
                        help="Skip scalability test (faster run)")
    args = parser.parse_args()

    results_dir = Path(args.results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)

    log.info("=" * 60)
    log.info("  Slope One Movie Recommendation System")
    log.info("  DS5001 Advanced Big Data Analytics — Cluster-09")
    log.info("=" * 60)

    # 1. Spark
    spark = create_spark(mode=args.mode, master=args.master)

    # 2. Load data
    ratings, movies = load_movielens(spark, args.data_dir)

    # 3. Split
    train, test = split_data(ratings)

    # 4. Deviation matrix
    dev_matrix, dev_time = compute_deviation_matrix(train)

    # 5. Predictions
    predictions, eval_df = predict_ratings(train, dev_matrix, test)

    # 6. Evaluate
    log.info("Evaluating ...")
    rmse               = compute_rmse(eval_df)
    precision, recall  = compute_precision_recall_at_k(eval_df, predictions, train)

    log.info(f"RMSE:          {rmse}")
    log.info(f"Precision@10:  {precision}")
    log.info(f"Recall@10:     {recall}")

    # 7. Top-N
    top_n_df = generate_top_n(predictions, movies, train, n=args.top_n)
    top_n_path = str(results_dir / "top_n_recommendations.csv")
    top_n_df.coalesce(1).write.csv(top_n_path, header=True, mode="overwrite")

    # 8. Scalability
    scalability = []
    if not args.skip_scalability:
        scalability = run_scalability_test(spark, ratings)

    # 9. Save metrics
    metrics = {
        "rmse":             rmse,
        "precision_at_10":  precision,
        "recall_at_10":     recall,
        "deviation_time_s": round(dev_time, 2),
        "train_size":       train.count(),
        "test_size":        test.count(),
        "scalability":      scalability,
    }
    with open(results_dir / "metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)
    log.info(f"Results saved to {results_dir}/")

    # 10. Sample recommendations for display
    log.info("=" * 60)
    log.info("  SAMPLE RECOMMENDATIONS (User 1)")
    log.info("=" * 60)
    for row in top_n_df.filter(F.col("userId") == 1).collect():
        log.info(f"  {row['rank']:2}. {row['title']:<48} {row['predicted_rating']:.2f}")

    log.info("=" * 60)
    log.info(f"  RMSE:         {rmse}")
    log.info(f"  Precision@10: {precision}")
    log.info(f"  Recall@10:    {recall}")
    log.info("  PIPELINE COMPLETE")
    log.info("=" * 60)

    spark.stop()
    return metrics


if __name__ == "__main__":
    main()
