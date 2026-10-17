-- ============================================================
-- FinFlow - Amazon Redshift Schema
-- ============================================================
-- Star schema optimized for analytical queries.
-- Distribution and sort keys are tuned for common query patterns.
--
-- Hadoop Equivalent: Hive partitioned tables with ORC/Parquet
-- AWS Equivalent: Amazon Redshift RA3 cluster with Spectrum
-- ============================================================

-- ─── Create Schema ────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS finflow;

-- ─── Dimension Tables ─────────────────────────────────────────────────────────

-- Dimension: Date (pre-generated calendar table for fast date lookups)
CREATE TABLE IF NOT EXISTS finflow.dim_date (
    date_key        INT          ENCODE az64 NOT NULL,
    full_date       DATE         ENCODE az64,
    year            SMALLINT     ENCODE az64,
    quarter         SMALLINT     ENCODE az64,
    month           SMALLINT     ENCODE az64,
    month_name      VARCHAR(10)  ENCODE bytedict,
    week_of_year    SMALLINT     ENCODE az64,
    day_of_month    SMALLINT     ENCODE az64,
    day_of_week     SMALLINT     ENCODE az64,
    day_name        VARCHAR(10)  ENCODE bytedict,
    is_weekend      BOOLEAN      ENCODE raw,
    is_holiday      BOOLEAN      ENCODE raw
)
DISTSTYLE ALL   -- Small table: broadcast to all nodes
SORTKEY (date_key);


-- Dimension: Merchant
CREATE TABLE IF NOT EXISTS finflow.dim_merchant (
    merchant_key    INT IDENTITY(1,1) ENCODE az64,
    merchant_name   VARCHAR(100) ENCODE lzo NOT NULL,
    category        VARCHAR(50)  ENCODE bytedict,
    sub_category    VARCHAR(50)  ENCODE bytedict,
    country_hq      VARCHAR(5)   ENCODE bytedict,
    risk_tier       VARCHAR(10)  ENCODE bytedict,  -- LOW / MEDIUM / HIGH
    created_at      TIMESTAMP    ENCODE az64,
    PRIMARY KEY (merchant_key)
)
DISTSTYLE ALL
SORTKEY (merchant_name);


-- Dimension: User (with privacy-safe hashed identifiers)
CREATE TABLE IF NOT EXISTS finflow.dim_user (
    user_key        INT IDENTITY(1,1) ENCODE az64,
    user_id         VARCHAR(20)  ENCODE lzo NOT NULL,
    registration_date DATE       ENCODE az64,
    account_tier    VARCHAR(10)  ENCODE bytedict,  -- BASIC / PREMIUM / BUSINESS
    country_of_reg  VARCHAR(5)   ENCODE bytedict,
    kyc_verified    BOOLEAN      ENCODE raw,
    risk_profile    VARCHAR(10)  ENCODE bytedict,
    PRIMARY KEY (user_key)
)
DISTSTYLE ALL
SORTKEY (user_id);


-- ─── Fact Table: Transactions ─────────────────────────────────────────────────
-- This is the main fact table — hundreds of millions of rows in production.
-- Distribution key: user_id → ensures user-level queries are node-local.
-- Sort key: transaction_timestamp → range-restricted date queries are fast.

CREATE TABLE IF NOT EXISTS finflow.fact_transactions (
    -- Surrogate key
    txn_sk                  BIGINT IDENTITY(1,1) ENCODE az64,

    -- Natural key
    transaction_id          VARCHAR(36)  ENCODE lzo NOT NULL,

    -- Foreign keys to dimensions
    date_key                INT          ENCODE az64,
    user_id                 VARCHAR(20)  ENCODE lzo NOT NULL,
    merchant_name           VARCHAR(100) ENCODE lzo,

    -- Time attributes
    transaction_timestamp   TIMESTAMP    ENCODE az64,
    transaction_year        SMALLINT     ENCODE az64,
    transaction_month       SMALLINT     ENCODE az64,
    transaction_day         SMALLINT     ENCODE az64,
    transaction_hour        SMALLINT     ENCODE az64,
    day_of_week             SMALLINT     ENCODE az64,

    -- Transaction attributes
    category                VARCHAR(50)  ENCODE bytedict,
    amount_usd              DECIMAL(12,2) ENCODE az64,
    amount_bucket           VARCHAR(10)  ENCODE bytedict,
    currency                VARCHAR(5)   ENCODE bytedict,
    payment_method          VARCHAR(20)  ENCODE bytedict,
    card_type               VARCHAR(20)  ENCODE bytedict,
    country                 VARCHAR(5)   ENCODE bytedict,
    status                  VARCHAR(15)  ENCODE bytedict,
    device_type             VARCHAR(15)  ENCODE bytedict,

    -- Fraud signals
    is_fraud                SMALLINT     ENCODE az64,
    fraud_reason            VARCHAR(200) ENCODE lzo,
    fraud_score             SMALLINT     ENCODE az64,
    risk_band               VARCHAR(10)  ENCODE bytedict,
    ml_fraud_flag           SMALLINT     ENCODE az64,

    -- Velocity features
    txn_count_last_1h       INT          ENCODE az64,
    txn_count_last_24h      INT          ENCODE az64,
    amount_sum_last_1h      DECIMAL(12,2) ENCODE az64,
    amount_sum_last_24h     DECIMAL(12,2) ENCODE az64,

    -- Metadata
    ingestion_timestamp     TIMESTAMP    ENCODE az64,
    pipeline_version        VARCHAR(10)  ENCODE bytedict,

    PRIMARY KEY (txn_sk),
    UNIQUE (transaction_id)
)
DISTKEY (user_id)
COMPOUND SORTKEY (transaction_timestamp, transaction_year, transaction_month, risk_band);


-- ─── Materialized Views ───────────────────────────────────────────────────────

-- Fraud summary by day + merchant (auto-refreshed)
CREATE MATERIALIZED VIEW finflow.mv_daily_fraud_summary
AUTO REFRESH YES AS
SELECT
    transaction_year,
    transaction_month,
    transaction_day,
    merchant_name,
    category,
    country,
    risk_band,
    COUNT(*)                    AS total_txns,
    SUM(ml_fraud_flag)          AS fraud_txns,
    SUM(amount_usd)             AS total_amount_usd,
    SUM(CASE WHEN ml_fraud_flag = 1 THEN amount_usd ELSE 0 END) AS fraud_exposure_usd,
    AVG(fraud_score)            AS avg_fraud_score
FROM finflow.fact_transactions
GROUP BY 1, 2, 3, 4, 5, 6, 7;


-- User risk profile view
CREATE MATERIALIZED VIEW finflow.mv_user_risk_profile
AUTO REFRESH YES AS
SELECT
    user_id,
    COUNT(*)                    AS lifetime_txns,
    SUM(amount_usd)             AS lifetime_spend_usd,
    AVG(amount_usd)             AS avg_txn_amount,
    MAX(fraud_score)            AS peak_fraud_score,
    AVG(fraud_score)            AS avg_fraud_score,
    SUM(ml_fraud_flag)          AS fraud_txn_count,
    ROUND(100.0 * SUM(ml_fraud_flag) / COUNT(*), 2) AS fraud_rate_pct,
    MAX(transaction_timestamp)  AS last_txn_timestamp
FROM finflow.fact_transactions
GROUP BY user_id;


-- ─── Late-Arriving Data Handler ───────────────────────────────────────────────
-- Stored procedure for upsert (handles incremental loads)

CREATE OR REPLACE PROCEDURE finflow.upsert_transactions(p_staging_table VARCHAR)
AS $$
BEGIN
    -- Delete existing records that will be updated
    EXECUTE 'DELETE FROM finflow.fact_transactions
             USING ' || p_staging_table || ' stg
             WHERE finflow.fact_transactions.transaction_id = stg.transaction_id';

    -- Insert new / updated records
    EXECUTE 'INSERT INTO finflow.fact_transactions
             SELECT * FROM ' || p_staging_table;

    RAISE INFO 'Upsert completed from %', p_staging_table;
END;
$$ LANGUAGE plpgsql;
