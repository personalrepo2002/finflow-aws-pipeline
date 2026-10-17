-- ============================================================
-- FinFlow - Fraud Analysis Queries (Athena / Redshift)
-- ============================================================
-- These queries demonstrate analytics on the financial data lake.
-- Use these during portfolio demos or interviews.
-- ============================================================


-- ─── 1. Daily Fraud Overview ──────────────────────────────────────────────────
-- High-level daily summary: total transactions, fraud rate, $ exposure

SELECT
    DATE(transaction_timestamp)      AS txn_date,
    COUNT(*)                         AS total_transactions,
    SUM(ml_fraud_flag)               AS flagged_fraud,
    ROUND(100.0 * SUM(ml_fraud_flag) / COUNT(*), 2) AS fraud_rate_pct,
    ROUND(SUM(CASE WHEN ml_fraud_flag = 1 THEN amount_usd ELSE 0 END), 2) AS fraud_exposure_usd,
    ROUND(SUM(amount_usd), 2)        AS total_volume_usd
FROM finflow_db.curated_transactions
WHERE transaction_year = 2024
GROUP BY DATE(transaction_timestamp)
ORDER BY txn_date DESC;


-- ─── 2. Top 10 Merchants by Fraud Exposure ────────────────────────────────────

SELECT
    merchant_name,
    category,
    COUNT(*)                         AS total_txns,
    SUM(ml_fraud_flag)               AS fraud_txns,
    ROUND(100.0 * SUM(ml_fraud_flag) / COUNT(*), 2) AS fraud_rate_pct,
    ROUND(SUM(CASE WHEN ml_fraud_flag = 1 THEN amount_usd ELSE 0 END), 2) AS fraud_exposure_usd
FROM finflow_db.curated_transactions
GROUP BY merchant_name, category
ORDER BY fraud_exposure_usd DESC
LIMIT 10;


-- ─── 3. Geographic Fraud Heatmap ─────────────────────────────────────────────

SELECT
    country,
    COUNT(*)                         AS total_txns,
    SUM(ml_fraud_flag)               AS fraud_count,
    ROUND(AVG(fraud_score), 1)       AS avg_fraud_score,
    ROUND(100.0 * SUM(ml_fraud_flag) / COUNT(*), 2) AS fraud_rate_pct
FROM finflow_db.curated_transactions
GROUP BY country
HAVING COUNT(*) > 100
ORDER BY avg_fraud_score DESC
LIMIT 20;


-- ─── 4. High-Risk User Analysis ──────────────────────────────────────────────
-- Users with the most HIGH-risk transactions (review for account action)

SELECT
    user_id,
    COUNT(*)                         AS total_txns,
    SUM(CASE WHEN risk_band = 'HIGH' THEN 1 ELSE 0 END)    AS high_risk_count,
    SUM(CASE WHEN risk_band = 'MEDIUM' THEN 1 ELSE 0 END)  AS medium_risk_count,
    MAX(fraud_score)                 AS max_fraud_score,
    ROUND(AVG(fraud_score), 1)       AS avg_fraud_score,
    ROUND(SUM(amount_usd), 2)        AS total_spent_usd
FROM finflow_db.curated_transactions
GROUP BY user_id
HAVING SUM(CASE WHEN risk_band = 'HIGH' THEN 1 ELSE 0 END) >= 3
ORDER BY high_risk_count DESC, max_fraud_score DESC
LIMIT 50;


-- ─── 5. Fraud by Hour of Day (Temporal Pattern) ───────────────────────────────

SELECT
    transaction_hour,
    COUNT(*)                         AS total_txns,
    SUM(ml_fraud_flag)               AS fraud_txns,
    ROUND(100.0 * SUM(ml_fraud_flag) / COUNT(*), 2) AS fraud_rate_pct,
    ROUND(AVG(amount_usd), 2)        AS avg_amount_usd
FROM finflow_db.curated_transactions
GROUP BY transaction_hour
ORDER BY transaction_hour;


-- ─── 6. Velocity Anomalies (Users with Suspicious Burst Activity) ─────────────

SELECT
    user_id,
    transaction_id,
    transaction_timestamp,
    merchant_name,
    amount_usd,
    txn_count_last_1h,
    amount_sum_last_1h,
    fraud_score,
    risk_band
FROM finflow_db.curated_transactions
WHERE txn_count_last_1h > 8
  AND risk_band IN ('HIGH', 'MEDIUM')
ORDER BY txn_count_last_1h DESC, fraud_score DESC
LIMIT 100;


-- ─── 7. Category-Level Risk Profile ──────────────────────────────────────────

SELECT
    category,
    COUNT(*)                                                AS total_txns,
    ROUND(AVG(amount_usd), 2)                              AS avg_amount,
    ROUND(SUM(amount_usd), 2)                              AS total_volume,
    SUM(CASE WHEN risk_band = 'HIGH'   THEN 1 ELSE 0 END)  AS high_risk,
    SUM(CASE WHEN risk_band = 'MEDIUM' THEN 1 ELSE 0 END)  AS medium_risk,
    ROUND(100.0 * SUM(ml_fraud_flag) / COUNT(*), 2)        AS fraud_rate_pct
FROM finflow_db.curated_transactions
GROUP BY category
ORDER BY fraud_rate_pct DESC;


-- ─── 8. Month-over-Month Fraud Trend ─────────────────────────────────────────

SELECT
    transaction_year,
    transaction_month,
    COUNT(*)                         AS total_txns,
    SUM(ml_fraud_flag)               AS fraud_txns,
    ROUND(100.0 * SUM(ml_fraud_flag) / COUNT(*), 2) AS fraud_rate_pct,
    ROUND(SUM(amount_usd), 2)        AS total_volume_usd,
    ROUND(SUM(CASE WHEN ml_fraud_flag = 1 THEN amount_usd ELSE 0 END), 2) AS fraud_exposure_usd
FROM finflow_db.curated_transactions
GROUP BY transaction_year, transaction_month
ORDER BY transaction_year, transaction_month;
