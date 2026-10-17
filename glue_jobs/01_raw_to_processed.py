"""
FinFlow - Glue ETL Job 1: Raw to Processed
============================================
Reads raw CSV transaction files from S3, performs data quality checks,
deduplication, schema enforcement, and writes Parquet to the processed layer.

AWS Glue PySpark Job — Deploy to AWS Glue Studio or via CLI.

Hadoop Equivalent: MapReduce cleansing job / Hive ETL + ORC writes
AWS Equivalent: AWS Glue PySpark with DynamicFrame

Author: [Your Name] | AWS Data Engineering Portfolio
"""

import sys
from datetime import datetime

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ─── Job Initialization ───────────────────────────────────────────────────────

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "SOURCE_BUCKET",
        "DEST_BUCKET",
        "SOURCE_PREFIX",
        "DEST_PREFIX",
        "GLUE_DATABASE",
        "GLUE_TABLE",
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger = glueContext.get_logger()
logger.info(f"[FinFlow] Job started: {args['JOB_NAME']} at {datetime.now().isoformat()}")

# ─── Schema Definition ────────────────────────────────────────────────────────

TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id",  StringType(),    nullable=False),
    StructField("timestamp",       StringType(),    nullable=False),
    StructField("date",            StringType(),    nullable=True),
    StructField("hour",            IntegerType(),   nullable=True),
    StructField("user_id",         StringType(),    nullable=False),
    StructField("merchant_name",   StringType(),    nullable=True),
    StructField("category",        StringType(),    nullable=True),
    StructField("amount",          DoubleType(),    nullable=False),
    StructField("currency",        StringType(),    nullable=True),
    StructField("payment_method",  StringType(),    nullable=True),
    StructField("card_type",       StringType(),    nullable=True),
    StructField("country",         StringType(),    nullable=True),
    StructField("status",          StringType(),    nullable=True),
    StructField("is_fraud",        IntegerType(),   nullable=True),
    StructField("fraud_reason",    StringType(),    nullable=True),
    StructField("device_type",     StringType(),    nullable=True),
    StructField("ip_address",      StringType(),    nullable=True),
    StructField("session_id",      StringType(),    nullable=True),
])

# ─── Step 1: Read Raw Data ────────────────────────────────────────────────────

source_path = f"s3://{args['SOURCE_BUCKET']}/{args['SOURCE_PREFIX']}/"
logger.info(f"[FinFlow] Reading raw data from: {source_path}")

raw_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .schema(TRANSACTION_SCHEMA) \
    .load(source_path)

raw_count = raw_df.count()
logger.info(f"[FinFlow] Raw record count: {raw_count:,}")

# ─── Step 2: Data Quality Checks ─────────────────────────────────────────────

# 2a. Flag records with nulls in critical fields
df_flagged = raw_df.withColumn(
    "dq_null_flag",
    F.when(
        F.col("transaction_id").isNull() |
        F.col("user_id").isNull() |
        F.col("amount").isNull() |
        F.col("timestamp").isNull(),
        F.lit(True)
    ).otherwise(F.lit(False))
)

# 2b. Filter out invalid amounts (negative or zero)
df_valid_amount = df_flagged.withColumn(
    "dq_invalid_amount_flag",
    F.when(F.col("amount") <= 0, F.lit(True)).otherwise(F.lit(False))
)

# 2c. Separate good vs bad records
df_good = df_valid_amount.filter(
    (~F.col("dq_null_flag")) & (~F.col("dq_invalid_amount_flag"))
)
df_bad = df_valid_amount.filter(
    F.col("dq_null_flag") | F.col("dq_invalid_amount_flag")
)

good_count = df_good.count()
bad_count = df_bad.count()
logger.info(f"[FinFlow] DQ check — Good: {good_count:,}, Rejected: {bad_count:,}")

# Write rejected records to quarantine zone
if bad_count > 0:
    quarantine_path = f"s3://{args['DEST_BUCKET']}/quarantine/transactions/"
    df_bad.write.mode("append").parquet(quarantine_path)
    logger.info(f"[FinFlow] Wrote {bad_count:,} rejected records to quarantine")

# ─── Step 3: Deduplication ────────────────────────────────────────────────────

# Deduplicate on transaction_id — keep most recent ingestion
df_deduped = df_good \
    .withColumn("row_num", F.row_number().over(
        __import__("pyspark.sql.window", fromlist=["Window"])
        .Window.partitionBy("transaction_id")
        .orderBy(F.col("timestamp").desc())
    )) \
    .filter(F.col("row_num") == 1) \
    .drop("row_num")

dedup_count = df_deduped.count()
dupes_removed = good_count - dedup_count
logger.info(f"[FinFlow] Deduplication — Removed {dupes_removed:,} duplicates. Remaining: {dedup_count:,}")

# ─── Step 4: Transformations & Enrichment ────────────────────────────────────

df_enriched = df_deduped \
    .drop("dq_null_flag", "dq_invalid_amount_flag") \
    .withColumn(
        "transaction_timestamp",
        F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd HH:mm:ss")
    ) \
    .withColumn("transaction_year",  F.year("transaction_timestamp")) \
    .withColumn("transaction_month", F.month("transaction_timestamp")) \
    .withColumn("transaction_day",   F.dayofmonth("transaction_timestamp")) \
    .withColumn("transaction_hour",  F.hour("transaction_timestamp")) \
    .withColumn("day_of_week",       F.dayofweek("transaction_timestamp")) \
    .withColumn(
        "amount_bucket",
        F.when(F.col("amount") < 10,    F.lit("micro"))
         .when(F.col("amount") < 100,   F.lit("small"))
         .when(F.col("amount") < 500,   F.lit("medium"))
         .when(F.col("amount") < 2000,  F.lit("large"))
         .otherwise(F.lit("very_large"))
    ) \
    .withColumn("amount_usd", F.round(F.col("amount"), 2)) \
    .withColumn("ingestion_timestamp", F.current_timestamp()) \
    .withColumn("pipeline_version", F.lit("1.0")) \
    .withColumn("source_system", F.lit("finflow_generator"))

# ─── Step 5: Write Processed Parquet ─────────────────────────────────────────

dest_path = f"s3://{args['DEST_BUCKET']}/{args['DEST_PREFIX']}/"
logger.info(f"[FinFlow] Writing processed data to: {dest_path}")

df_enriched.write \
    .mode("overwrite") \
    .partitionBy("transaction_year", "transaction_month", "transaction_day") \
    .parquet(dest_path)

logger.info(f"[FinFlow] ✅ Step 1 Complete — Wrote {dedup_count:,} records to processed layer")

# ─── Step 6: Update Glue Data Catalog ────────────────────────────────────────

dynamic_frame = DynamicFrame.fromDF(df_enriched, glueContext, "processed_transactions")

glueContext.write_dynamic_frame.from_catalog(
    frame=dynamic_frame,
    database=args["GLUE_DATABASE"],
    table_name=args["GLUE_TABLE"],
    additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"},
)

# ─── Job Metrics ──────────────────────────────────────────────────────────────

print(f"""
╔══════════════════════════════════════════════════╗
║         FinFlow - Job 1 Summary                  ║
╠══════════════════════════════════════════════════╣
║  Raw Records Read    : {raw_count:>12,}           ║
║  DQ Rejections       : {bad_count:>12,}           ║
║  Duplicates Removed  : {dupes_removed:>12,}           ║
║  Records Written     : {dedup_count:>12,}           ║
╚══════════════════════════════════════════════════╝
""")

job.commit()
