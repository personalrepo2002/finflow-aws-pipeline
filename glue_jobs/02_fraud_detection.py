"""
FinFlow - Glue ETL Job 2: Fraud Detection & Risk Scoring
=========================================================
Reads processed transactions from S3, applies advanced fraud detection rules
and velocity checks, computes risk scores, and writes curated output.

Key Techniques Demonstrated:
  - PySpark Window Functions (velocity checks per user over time windows)
  - User-level behavioral aggregations (ML feature engineering style)
  - Rule-based anomaly detection (industry-standard approach)
  - Chained transformations using PySpark DataFrames

Hadoop Equivalent: Hive/Pig analytics jobs with GROUP BY, self-joins
AWS Equivalent: AWS Glue PySpark with Window Functions

Author: [Your Name] | AWS Data Engineering Portfolio
"""

import sys
from datetime import datetime

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ─── Job Initialization ───────────────────────────────────────────────────────

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "SOURCE_BUCKET",
        "DEST_BUCKET",
        "SOURCE_PREFIX",
        "DEST_PREFIX",
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger = glueContext.get_logger()
logger.info(f"[FinFlow] Fraud Detection Job started at {datetime.now().isoformat()}")

# ─── Read Processed Data ──────────────────────────────────────────────────────

source_path = f"s3://{args['SOURCE_BUCKET']}/{args['SOURCE_PREFIX']}/"
df = spark.read.parquet(source_path)

total_count = df.count()
logger.info(f"[FinFlow] Loaded {total_count:,} transactions for fraud analysis")

# ─── Window Definitions for Velocity Checks ───────────────────────────────────

# 1-hour rolling window per user (ordered by timestamp)
user_time_window_1h = Window \
    .partitionBy("user_id") \
    .orderBy(F.col("transaction_timestamp").cast("long")) \
    .rangeBetween(-3600, 0)  # 3600 seconds = 1 hour

# 24-hour rolling window per user
user_time_window_24h = Window \
    .partitionBy("user_id") \
    .orderBy(F.col("transaction_timestamp").cast("long")) \
    .rangeBetween(-86400, 0)  # 86400 seconds = 24 hours

# All-time window per user (for cumulative aggregations)
user_all_time = Window.partitionBy("user_id")

# ─── Feature Engineering: Velocity Metrics ────────────────────────────────────

df_velocity = df \
    .withColumn(
        "txn_count_last_1h",
        F.count("transaction_id").over(user_time_window_1h)
    ) \
    .withColumn(
        "txn_count_last_24h",
        F.count("transaction_id").over(user_time_window_24h)
    ) \
    .withColumn(
        "amount_sum_last_1h",
        F.sum("amount_usd").over(user_time_window_1h)
    ) \
    .withColumn(
        "amount_sum_last_24h",
        F.sum("amount_usd").over(user_time_window_24h)
    ) \
    .withColumn(
        "unique_countries_last_24h",
        F.approx_count_distinct("country").over(user_time_window_24h)
    ) \
    .withColumn(
        "unique_merchants_last_1h",
        F.approx_count_distinct("merchant_name").over(user_time_window_1h)
    )

# ─── Feature Engineering: User Behavioral Baseline ───────────────────────────

df_baseline = df_velocity \
    .withColumn("user_avg_amount",   F.avg("amount_usd").over(user_all_time)) \
    .withColumn("user_stddev_amount", F.stddev("amount_usd").over(user_all_time)) \
    .withColumn("user_total_txns",    F.count("transaction_id").over(user_all_time)) \
    .withColumn(
        "amount_zscore",
        F.when(
            F.col("user_stddev_amount") > 0,
            (F.col("amount_usd") - F.col("user_avg_amount")) / F.col("user_stddev_amount")
        ).otherwise(F.lit(0.0))
    )

# ─── Rule-Based Fraud Scoring ─────────────────────────────────────────────────
# Each rule adds points to a fraud_score (0-100 scale)

HIGH_RISK_COUNTRIES = ["NG", "RU", "CN", "KP", "IR"]

df_scored = df_baseline \
    .withColumn("score_high_amount",
        F.when(F.col("amount_usd") > 5000, F.lit(30))
         .when(F.col("amount_usd") > 2000, F.lit(15))
         .otherwise(F.lit(0))
    ) \
    .withColumn("score_velocity_1h",
        F.when(F.col("txn_count_last_1h") > 10, F.lit(25))
         .when(F.col("txn_count_last_1h") > 5,  F.lit(15))
         .otherwise(F.lit(0))
    ) \
    .withColumn("score_amount_velocity_1h",
        F.when(F.col("amount_sum_last_1h") > 10000, F.lit(20))
         .when(F.col("amount_sum_last_1h") > 5000,  F.lit(10))
         .otherwise(F.lit(0))
    ) \
    .withColumn("score_geo_anomaly",
        F.when(F.col("unique_countries_last_24h") >= 3, F.lit(20))
         .when(F.col("unique_countries_last_24h") == 2, F.lit(10))
         .otherwise(F.lit(0))
    ) \
    .withColumn("score_high_risk_country",
        F.when(F.col("country").isin(HIGH_RISK_COUNTRIES), F.lit(15))
         .otherwise(F.lit(0))
    ) \
    .withColumn("score_zscore_anomaly",
        F.when(F.col("amount_zscore") > 3.0, F.lit(20))
         .when(F.col("amount_zscore") > 2.0, F.lit(10))
         .otherwise(F.lit(0))
    ) \
    .withColumn("score_micro_transactions",
        F.when(
            (F.col("amount_usd") < 1.5) & (F.col("txn_count_last_1h") > 3),
            F.lit(20)
        ).otherwise(F.lit(0))
    ) \
    .withColumn("score_odd_hours",
        # Transactions between 2 AM - 5 AM
        F.when(F.col("transaction_hour").between(2, 5), F.lit(5))
         .otherwise(F.lit(0))
    )

# ─── Compute Total Fraud Score & Risk Band ────────────────────────────────────

score_columns = [
    "score_high_amount", "score_velocity_1h", "score_amount_velocity_1h",
    "score_geo_anomaly", "score_high_risk_country", "score_zscore_anomaly",
    "score_micro_transactions", "score_odd_hours"
]

df_final = df_scored \
    .withColumn(
        "fraud_score",
        F.least(
            sum([F.col(c) for c in score_columns]),
            F.lit(100)  # Cap at 100
        )
    ) \
    .withColumn(
        "risk_band",
        F.when(F.col("fraud_score") >= 60, F.lit("HIGH"))
         .when(F.col("fraud_score") >= 30, F.lit("MEDIUM"))
         .when(F.col("fraud_score") >= 10, F.lit("LOW"))
         .otherwise(F.lit("MINIMAL"))
    ) \
    .withColumn(
        "ml_fraud_flag",
        F.when(F.col("fraud_score") >= 60, F.lit(1)).otherwise(F.lit(0))
    ) \
    .withColumn("fraud_analysis_timestamp", F.current_timestamp())

# ─── Write Curated Output ─────────────────────────────────────────────────────

dest_path = f"s3://{args['DEST_BUCKET']}/{args['DEST_PREFIX']}/"
logger.info(f"[FinFlow] Writing fraud-scored data to: {dest_path}")

df_final.write \
    .mode("overwrite") \
    .partitionBy("transaction_year", "transaction_month", "risk_band") \
    .parquet(dest_path)

# ─── Log Fraud Statistics ─────────────────────────────────────────────────────

fraud_stats = df_final.groupBy("risk_band").agg(
    F.count("*").alias("count"),
    F.avg("fraud_score").alias("avg_score"),
    F.sum("amount_usd").alias("total_amount")
).collect()

print("\n" + "═" * 55)
print("  FinFlow - Fraud Detection Summary")
print("═" * 55)
print(f"  {'Risk Band':<12} {'Count':>10} {'Avg Score':>10} {'Total $':>12}")
print("  " + "─" * 50)
for row in sorted(fraud_stats, key=lambda r: -r["count"]):
    print(f"  {row['risk_band']:<12} {row['count']:>10,} {row['avg_score']:>10.1f} {row['total_amount']:>12,.2f}")
print("═" * 55 + "\n")

job.commit()
