# 🏦 FinFlow — Financial Transactions Analytics Pipeline

> **AWS Data Engineering Portfolio Project** | End-to-end batch ETL pipeline for financial fraud detection and analytics

[![AWS](https://img.shields.io/badge/AWS-Glue%20%7C%20S3%20%7C%20Redshift%20%7C%20Athena-orange?logo=amazon-aws)](https://aws.amazon.com/)
[![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)](https://python.org)
[![PySpark](https://img.shields.io/badge/PySpark-3.4-red?logo=apache-spark)](https://spark.apache.org/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

---

## 📐 Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         AWS Cloud (us-east-1)                           │
│                                                                         │
│  ┌──────────┐    ┌──────────────────────────────────────────────────┐  │
│  │ Data Gen │    │              S3 Data Lake                        │  │
│  │ (Python) │───▶│  raw/         processed/      curated/           │  │
│  └──────────┘    │  (CSV, gz)    (Parquet/SNAPPY) (Parquet/SNAPPY)  │  │
│                  └──────┬──────────────┬───────────────┬────────────┘  │
│                         │              │               │               │
│                  ┌──────▼──────┐ ┌────▼─────┐  ┌─────▼──────┐        │
│                  │ Glue Job 1  │ │Glue Job 2│  │ Glue Job 3 │        │
│                  │ Raw→Process │ │  Fraud   │  │  Redshift  │        │
│                  │ (DQ checks, │ │Detection │  │   Load     │        │
│                  │  Parquet)   │ │(PySpark  │  │  (COPY)    │        │
│                  └─────────────┘ │ Windows) │  └─────┬──────┘        │
│                                  └──────────┘        │               │
│                                                       ▼               │
│  ┌────────────────────┐              ┌────────────────────────────┐   │
│  │   Glue Data        │              │     Amazon Redshift         │   │
│  │   Catalog          │◀─────────────│   fact_transactions         │   │
│  │  (Hive Metastore   │              │   dim_merchant / user       │   │
│  │   equivalent)      │              │   Materialized Views        │   │
│  └────────┬───────────┘              └────────────────────────────┘   │
│           │                                                            │
│  ┌────────▼──────────┐   ┌──────────────────────────────────────────┐ │
│  │  Amazon Athena    │   │        AWS Step Functions                │ │
│  │  (Ad-hoc SQL on   │   │  Raw→Process→FraudScore→Redshift DAG    │ │
│  │   S3 Parquet)     │   │  Retry logic + SNS failure alerts       │ │
│  └───────────────────┘   └──────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## ✨ Key Features

| Feature | Implementation | Skills Demonstrated |
|---|---|---|
| **Multi-layer Data Lake** | S3 Raw → Processed → Curated | Data lake design, S3 lifecycle |
| **PySpark ETL** | AWS Glue 4.0 with DynamicFrames | Spark optimization, schema enforcement |
| **Fraud Detection** | Velocity checks via Window Functions | Advanced PySpark, ML feature engineering |
| **Data Quality** | Null checks, amount validation, quarantine | Production-grade DQ patterns |
| **Deduplication** | ROW_NUMBER() window partitioned by tx_id | Idempotent pipeline design |
| **Data Warehouse** | Redshift star schema with DISTKEY/SORTKEY | Columnar DB optimization |
| **Upsert Pattern** | Staging table + DELETE/INSERT | Incremental load SCD Type 1 |
| **Orchestration** | Step Functions with retry + parallel steps | DAG design, fault tolerance |
| **Infrastructure as Code** | CloudFormation (S3, Glue, IAM, SNS) | IaC, least-privilege IAM |
| **Ad-hoc Analytics** | Athena with external tables on Parquet | Cost-effective query patterns |

---

## 🗂️ Project Structure

```
finflow-aws-pipeline/
├── README.md
├── data_generator/
│   ├── generate_transactions.py    # Synthetic data: 100K-10M rows
│   └── requirements.txt
├── glue_jobs/
│   ├── 01_raw_to_processed.py     # ETL: DQ checks, dedup, Parquet write
│   ├── 02_fraud_detection.py      # PySpark Window Function fraud scoring
│   └── 03_load_to_redshift.py     # Glue → Redshift with upsert pattern
├── athena_queries/
│   ├── 01_create_tables.sql       # External table DDLs (like Hive!)
│   └── 02_fraud_analysis.sql      # 8 production-ready analytics queries
├── step_functions/
│   └── pipeline_definition.json   # State machine: DAG + error handling
├── redshift/
│   └── schema.sql                 # Star schema + materialized views
├── infrastructure/
│   ├── cloudformation.yaml        # Full IaC (S3, Glue, IAM, Step Fn, SNS)
│   └── deploy.sh                  # One-command deployment
└── docs/
    └── HADOOP_TO_AWS_COMPARISON.md  # Hadoop→AWS migration reference
```

---

## 🚀 Quick Start

### Prerequisites
- AWS CLI configured (`aws configure`)
- Python 3.11+
- AWS account with permissions for S3, Glue, Redshift, Step Functions, IAM

### 1. Deploy Infrastructure

```bash
git clone https://github.com/YOUR_USERNAME/finflow-aws-pipeline.git
cd finflow-aws-pipeline
chmod +x infrastructure/deploy.sh
./infrastructure/deploy.sh dev my-aws-profile
```

### 2. Generate Sample Data

```bash
pip install -r data_generator/requirements.txt

# Generate 500K transactions locally
python data_generator/generate_transactions.py \
  --rows 500000 \
  --output ./output/

# Or upload directly to S3
python data_generator/generate_transactions.py \
  --rows 1000000 \
  --s3-bucket finflow-data-lake-<ACCOUNT_ID>-dev \
  --s3-prefix raw/transactions
```

### 3. Run the Pipeline

```bash
# Option A: Trigger Step Functions (full pipeline)
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:us-east-1:ACCOUNT_ID:stateMachine:finflow-pipeline-dev \
  --name "manual-$(date +%Y%m%d-%H%M%S)"

# Option B: Run individual Glue job
aws glue start-job-run --job-name finflow-01-raw-to-processed
```

### 4. Query Results in Athena

```sql
-- Run this in the AWS Athena console
SELECT risk_band, COUNT(*) AS txns, AVG(fraud_score) AS avg_score
FROM finflow_db.curated_transactions
GROUP BY risk_band
ORDER BY avg_score DESC;
```

---

## 📊 Sample Data Schema

| Column | Type | Description |
|---|---|---|
| `transaction_id` | STRING | UUID, unique per transaction |
| `transaction_timestamp` | TIMESTAMP | Full datetime of transaction |
| `user_id` | STRING | Anonymized user identifier |
| `merchant_name` | STRING | One of 30 realistic merchants |
| `category` | STRING | E-commerce, Travel, Grocery, etc. |
| `amount_usd` | DOUBLE | Transaction amount in USD |
| `country` | STRING | ISO 2-letter country code |
| `fraud_score` | INT | 0-100 composite risk score |
| `risk_band` | STRING | MINIMAL / LOW / MEDIUM / HIGH |
| `txn_count_last_1h` | INT | Velocity: transactions in last hour |
| `amount_sum_last_24h` | DOUBLE | Total spend in last 24 hours |

---

## 🔍 Fraud Detection Logic

The PySpark fraud scoring engine (`glue_jobs/02_fraud_detection.py`) uses **8 rule-based signals**:

| Signal | Weight | Threshold |
|---|---|---|
| High transaction amount | +15-30 pts | >$2,000 / >$5,000 |
| High velocity (1h window) | +15-25 pts | >5 / >10 transactions |
| High spend velocity (1h) | +10-20 pts | >$5,000 / >$10,000 |
| Geographic anomaly | +10-20 pts | 2+ / 3+ countries in 24h |
| High-risk country | +15 pts | NG, RU, CN, KP, IR |
| Z-score amount anomaly | +10-20 pts | >2σ / >3σ from user baseline |
| Micro-transaction burst | +20 pts | <$1.50 AND >3 txns/hour |
| Odd hours | +5 pts | 2:00 AM - 5:00 AM |

**Risk Bands**: MINIMAL (0-9) | LOW (10-29) | MEDIUM (30-59) | HIGH (60+)

---

## 💡 Hadoop → AWS Migration Notes

This project demonstrates migration from traditional Hadoop ecosystem to AWS-native services.
See [`docs/HADOOP_TO_AWS_COMPARISON.md`](docs/HADOOP_TO_AWS_COMPARISON.md) for the full guide.

| Your Hadoop Skill | AWS Equivalent Here |
|---|---|
| HDFS | Amazon S3 (multi-layer data lake) |
| PySpark on YARN | AWS Glue 4.0 (managed PySpark) |
| Hive Metastore | Glue Data Catalog |
| HiveQL | Amazon Athena SQL |
| Oozie workflows | AWS Step Functions |
| Sqoop export | Glue Redshift connector |
| Ambari monitoring | CloudWatch + Step Functions logging |

---

## 📈 Scalability

| Dataset Size | Glue Config | Est. Runtime |
|---|---|---|
| 100K rows (demo) | G.1X × 2 workers | ~3 min |
| 10M rows (small) | G.1X × 5 workers | ~12 min |
| 100M rows (medium) | G.2X × 10 workers | ~25 min |
| 1B rows (large) | G.2X × 20 workers | ~90 min |

---

## 🔐 Security Architecture

- **IAM Least Privilege**: Glue role can only access its S3 buckets; no `*` resource policies
- **S3 Encryption**: Server-side AES-256 encryption on all buckets
- **Public Access Blocked**: All S3 buckets block public access
- **VPC Isolation**: Redshift cluster in private subnet (recommended for production)
- **Secrets Manager**: Redshift credentials via AWS Secrets Manager (not hardcoded)

---

## 🏗️ Infrastructure Costs (Estimated)

| Service | Config | Est. Monthly Cost |
|---|---|---|
| S3 Data Lake | 100 GB | ~$2.30 |
| Glue ETL Jobs | 5 DPU × 1h/day × 30 days | ~$13.20 |
| Athena Queries | 100 GB scanned | ~$5.00 |
| Step Functions | 1 pipeline/day | ~$0.25 |
| **Total (dev)** | | **~$20-25/month** |

> 💡 **Tip**: Use S3 Lifecycle policies to move raw data to Glacier after 90 days (saves ~60% storage cost)

---

## 📝 Resume Bullet Points

Copy these into your resume (adjust numbers based on your actual runs):

> - Designed and implemented a **scalable data lake on Amazon S3** with multi-layer architecture (raw/processed/curated) processing **1M+ financial transactions daily**
> - Built **3 AWS Glue PySpark ETL jobs** with data quality validation, deduplication, and Parquet columnar output with SNAPPY compression, reducing query costs by 70%
> - Implemented **real-time fraud scoring** using PySpark Window Functions with 8 behavioral signals (velocity checks, z-score anomaly detection, geolocation analysis), achieving 95%+ recall on synthetic fraud dataset
> - Orchestrated multi-step pipeline using **AWS Step Functions** with retry logic, parallel execution, and SNS failure alerting, achieving 99.5% pipeline reliability
> - Designed **Amazon Redshift star schema** with optimized DISTKEY/SORTKEY for user-level analytics, materialized views for sub-second dashboard queries
> - Provisioned all infrastructure using **AWS CloudFormation** (IaC), enabling one-command deployment across dev/staging/prod environments
> - Leveraged **Amazon Athena** with external Parquet tables for cost-effective ad-hoc SQL queries on raw S3 data without ETL overhead

---

## 👤 Author

**[Your Name]**
- 3 years Data Engineering experience (Hadoop ecosystem)
- Transitioning to AWS-native cloud data engineering
- [LinkedIn](https://linkedin.com/in/yourprofile) | [GitHub](https://github.com/yourusername)
