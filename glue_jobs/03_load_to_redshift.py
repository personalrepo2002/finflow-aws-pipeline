"""
FinFlow - Glue ETL Job 3: Load to Amazon Redshift
==================================================
Reads curated Parquet data from S3 and loads it into Amazon Redshift
using the optimized COPY command via S3 staging.

Demonstrates:
  - Upsert (MERGE) pattern for incremental loads
  - Redshift-optimized column ordering / compression
  - Glue Redshift connector with JDBC connection
  - Staging table pattern for atomic loads

Hadoop Equivalent: Sqoop export / HBase bulk load
AWS Equivalent: Glue Redshift connector + COPY command

Author: [Your Name] | AWS Data Engineering Portfolio
"""

import sys
from datetime import datetime

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# ─── Job Initialization ───────────────────────────────────────────────────────

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "SOURCE_BUCKET",
        "SOURCE_PREFIX",
        "REDSHIFT_CONNECTION",
        "REDSHIFT_DB",
        "REDSHIFT_SCHEMA",
        "REDSHIFT_TMP_DIR",
        "LOAD_MODE",  # "full" or "incremental"
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger = glueContext.get_logger()
logger.info(f"[FinFlow] Redshift Load Job started. Mode: {args['LOAD_MODE']}")

# ─── Read Curated Data ────────────────────────────────────────────────────────

source_path = f"s3://{args['SOURCE_BUCKET']}/{args['SOURCE_PREFIX']}/"
df = spark.read.parquet(source_path)

# Select and rename columns for Redshift target schema
df_redshift = df.select(
    F.col("transaction_id"),
    F.col("transaction_timestamp"),
    F.col("transaction_year"),
    F.col("transaction_month"),
    F.col("transaction_day"),
    F.col("transaction_hour"),
    F.col("day_of_week"),
    F.col("user_id"),
    F.col("merchant_name"),
    F.col("category"),
    F.col("amount_usd"),
    F.col("amount_bucket"),
    F.col("currency"),
    F.col("payment_method"),
    F.col("card_type"),
    F.col("country"),
    F.col("status"),
    F.col("is_fraud"),
    F.col("fraud_reason"),
    F.col("fraud_score"),
    F.col("risk_band"),
    F.col("ml_fraud_flag"),
    F.col("txn_count_last_1h"),
    F.col("txn_count_last_24h"),
    F.col("amount_sum_last_1h"),
    F.col("amount_sum_last_24h"),
    F.col("device_type"),
    F.col("ingestion_timestamp"),
    F.col("pipeline_version"),
)

record_count = df_redshift.count()
logger.info(f"[FinFlow] Records to load: {record_count:,}")

# ─── Write to Redshift ────────────────────────────────────────────────────────

target_table = f"{args['REDSHIFT_SCHEMA']}.fact_transactions"

if args["LOAD_MODE"] == "full":
    # Full load: truncate and reload
    pre_actions = f"TRUNCATE TABLE {target_table};"
    post_actions = f"""
        ANALYZE {target_table};
        VACUUM SORT ONLY {target_table};
    """
else:
    # Incremental: DELETE existing records for the same dates, then insert
    # This is the standard SCD Type 1 upsert pattern for Redshift
    staging_table = f"{args['REDSHIFT_SCHEMA']}.fact_transactions_staging"
    pre_actions = f"""
        DROP TABLE IF EXISTS {staging_table};
        CREATE TABLE {staging_table} (LIKE {target_table});
    """
    post_actions = f"""
        DELETE FROM {target_table}
        USING {staging_table}
        WHERE {target_table}.transaction_id = {staging_table}.transaction_id;

        INSERT INTO {target_table}
        SELECT * FROM {staging_table};

        DROP TABLE {staging_table};
        ANALYZE {target_table};
    """
    target_table = staging_table

glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=glueContext.create_dynamic_frame.from_dataframe(df_redshift, glueContext),
    catalog_connection=args["REDSHIFT_CONNECTION"],
    connection_options={
        "database": args["REDSHIFT_DB"],
        "dbtable": target_table,
        "aws_iam_role": "arn:aws:iam::ACCOUNT_ID:role/FinFlowRedshiftRole",  # Update with your ARN
        "preactions": pre_actions,
        "postactions": post_actions,
    },
    redshift_tmp_dir=args["REDSHIFT_TMP_DIR"],
)

logger.info(f"[FinFlow] ✅ Successfully loaded {record_count:,} records to Redshift")

print(f"""
╔══════════════════════════════════════════════════╗
║       FinFlow - Redshift Load Summary            ║
╠══════════════════════════════════════════════════╣
║  Records Loaded  : {record_count:>15,}            ║
║  Target Table    : {target_table:<25}   ║
║  Load Mode       : {args["LOAD_MODE"]:<25}   ║
║  Completed At    : {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}          ║
╚══════════════════════════════════════════════════╝
""")

job.commit()
