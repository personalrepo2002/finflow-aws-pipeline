-- ============================================================
-- FinFlow - Athena External Tables (Data Lake Queries)
-- ============================================================
-- These tables point directly to S3 Parquet data — no ETL needed.
-- Equivalent to: Hive external tables on HDFS (your Hadoop experience!)
-- ============================================================

-- Create Glue Database (run once)
CREATE DATABASE IF NOT EXISTS finflow_db
  COMMENT 'FinFlow Financial Analytics Data Lake';

-- ─── Raw Transactions Table ───────────────────────────────────────────────────
-- Points to raw CSV files; useful for debugging and reprocessing

CREATE EXTERNAL TABLE IF NOT EXISTS finflow_db.raw_transactions (
    transaction_id  STRING,
    `timestamp`     STRING,
    `date`          STRING,
    `hour`          INT,
    user_id         STRING,
    merchant_name   STRING,
    category        STRING,
    amount          DOUBLE,
    currency        STRING,
    payment_method  STRING,
    card_type       STRING,
    country         STRING,
    status          STRING,
    is_fraud        INT,
    fraud_reason    STRING,
    device_type     STRING,
    ip_address      STRING,
    session_id      STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 's3://YOUR-BUCKET/raw/transactions/'
TBLPROPERTIES ('skip.header.line.count'='1');


-- ─── Processed Transactions Table ────────────────────────────────────────────
-- Parquet, partitioned by year/month/day — fast columnar reads

CREATE EXTERNAL TABLE IF NOT EXISTS finflow_db.processed_transactions (
    transaction_id          STRING,
    transaction_timestamp   TIMESTAMP,
    user_id                 STRING,
    merchant_name           STRING,
    category                STRING,
    amount_usd              DOUBLE,
    amount_bucket           STRING,
    currency                STRING,
    payment_method          STRING,
    card_type               STRING,
    country                 STRING,
    status                  STRING,
    is_fraud                INT,
    fraud_reason            STRING,
    device_type             STRING,
    ip_address              STRING,
    session_id              STRING,
    ingestion_timestamp     TIMESTAMP,
    pipeline_version        STRING,
    source_system           STRING
)
PARTITIONED BY (
    transaction_year  INT,
    transaction_month INT,
    transaction_day   INT
)
STORED AS PARQUET
LOCATION 's3://YOUR-BUCKET/processed/transactions/'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY',
    'classification'      = 'parquet'
);

-- Load partitions from S3 (run after new data arrives)
MSCK REPAIR TABLE finflow_db.processed_transactions;


-- ─── Curated / Fraud-Scored Table ────────────────────────────────────────────

CREATE EXTERNAL TABLE IF NOT EXISTS finflow_db.curated_transactions (
    transaction_id          STRING,
    transaction_timestamp   TIMESTAMP,
    transaction_hour        INT,
    day_of_week             INT,
    user_id                 STRING,
    merchant_name           STRING,
    category                STRING,
    amount_usd              DOUBLE,
    amount_bucket           STRING,
    currency                STRING,
    payment_method          STRING,
    card_type               STRING,
    country                 STRING,
    status                  STRING,
    is_fraud                INT,
    fraud_reason            STRING,
    fraud_score             INT,
    ml_fraud_flag           INT,
    txn_count_last_1h       BIGINT,
    txn_count_last_24h      BIGINT,
    amount_sum_last_1h      DOUBLE,
    amount_sum_last_24h     DOUBLE,
    device_type             STRING,
    ingestion_timestamp     TIMESTAMP
)
PARTITIONED BY (
    transaction_year  INT,
    transaction_month INT,
    risk_band         STRING
)
STORED AS PARQUET
LOCATION 's3://YOUR-BUCKET/curated/transactions/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

MSCK REPAIR TABLE finflow_db.curated_transactions;
