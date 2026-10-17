# Hadoop → AWS Migration Reference Guide
# FinFlow Data Engineering Portfolio

This document maps your **Hadoop ecosystem experience** to equivalent AWS services.
Use this during interviews to demonstrate technology breadth and migration knowledge.

---

## Component Mapping: Hadoop vs AWS

| Hadoop Ecosystem | AWS Equivalent | FinFlow Usage |
|---|---|---|
| **HDFS** | **Amazon S3** | Raw/Processed/Curated data layers |
| **MapReduce** | **AWS Glue (PySpark)** | ETL jobs 1, 2, 3 |
| **YARN** | **AWS Glue (managed)** | Resource management (fully managed) |
| **Hive** | **AWS Glue Data Catalog + Athena** | Schema management + SQL queries |
| **HiveQL** | **Amazon Athena SQL** | Fraud analysis queries |
| **ORC/Parquet on HDFS** | **Parquet on S3** | Curated layer storage format |
| **Oozie** | **AWS Step Functions** | Pipeline orchestration DAG |
| **HBase** | **Amazon DynamoDB** | (low-latency lookups) |
| **Pig** | **AWS Glue PySpark** | Data transformations |
| **Sqoop** | **AWS Glue JDBC connector** | Redshift loads |
| **Hue** | **Amazon Athena Console / QuickSight** | Query UI |
| **Ambari** | **AWS CloudFormation + CloudWatch** | Infrastructure monitoring |
| **Kafka** | **Amazon Kinesis** | Streaming (future extension) |
| **Zookeeper** | **AWS Managed (built into services)** | Coordination (abstracted away) |

---

## Key Architectural Differences

### 1. Storage: HDFS vs S3

**Hadoop (HDFS)**:
- Tightly coupled compute + storage (data lives on cluster nodes)
- Must keep cluster running 24/7 to access data
- Scaling requires adding more nodes (expensive)
- Replication factor = 3× storage overhead

**AWS (S3)**:
- Fully decoupled compute and storage
- Storage is always available; pay only for active compute
- Scale to petabytes instantly, no provisioning needed
- 99.999999999% (11 nines) durability built-in
- Native versioning, lifecycle policies, cross-region replication

```
# Hadoop HDFS command
hdfs dfs -put transactions.csv /user/data/raw/

# AWS S3 equivalent  
aws s3 cp transactions.csv s3://finflow-data-lake/raw/transactions/
```

### 2. Processing: MapReduce vs Glue PySpark

**Hadoop MapReduce**:
- Low-level Map and Reduce phases — lots of boilerplate
- Data serialized to disk between phases (slow)
- JVM-based; complex dependency management

**AWS Glue PySpark**:
- High-level DataFrame API (same as on-prem Spark)
- In-memory processing with lazy evaluation
- Fully managed: no cluster setup, auto-scaling
- Native integration with S3, Glue Catalog, Redshift

```python
# Hadoop MapReduce style (pseudo-code)
class FraudMapper(Mapper):
    def map(self, key, value, context):
        amount = float(value.split(',')[7])
        if amount > 5000:
            context.write(key, 'HIGH_RISK')

# AWS Glue PySpark equivalent (much cleaner!)
df = spark.read.parquet("s3://bucket/processed/")
df_flagged = df.withColumn("risk_band",
    F.when(F.col("amount") > 5000, "HIGH")
     .otherwise("LOW")
)
```

### 3. Orchestration: Oozie vs Step Functions

**Apache Oozie**:
- XML-based workflow definitions (verbose, error-prone)
- Must deploy coordinator jobs for scheduling
- Limited built-in error handling and retry logic
- Requires Hadoop cluster to run

**AWS Step Functions**:
- JSON-based state machine (cleaner, versionable)
- Native retry with exponential backoff
- Parallel execution, choice states, error catching
- Serverless — no infrastructure to manage
- Visual workflow editor in AWS Console

### 4. Schema Management: Hive Metastore vs Glue Catalog

**Hive Metastore**:
- Local metastore (often MySQL) tied to cluster
- Schema changes require DDL statements
- Not accessible outside the Hadoop cluster

**AWS Glue Data Catalog**:
- Centralized metadata store accessible by all AWS services
- Athena, EMR, Glue, Redshift Spectrum all share one catalog
- Schema versions tracked automatically
- Crawlers auto-discover schema from S3 data

---

## Interview Talking Points

### "Tell me about a Hadoop migration you've worked on"

> "In my current role, I work with Hadoop/HDFS for batch processing. For this project,
> I redesigned the architecture using AWS-native services to eliminate infrastructure
> management overhead. I mapped HDFS to S3 for decoupled storage, replaced our
> MapReduce jobs with AWS Glue PySpark (same Spark API, fully managed), moved from
> Oozie to Step Functions for orchestration, and used the Glue Data Catalog as a
> drop-in replacement for our Hive Metastore. The result is a pipeline that scales
> elastically and costs ~40% less because we only pay for active compute."

### "How does Parquet on S3 compare to your Hive setup?"

> "Both use Parquet/ORC with columnar compression. The key difference is that S3
> stores data independently from compute, so we can query it simultaneously from
> Athena for ad-hoc analysis, Redshift Spectrum for BI, and Glue for ETL — all
> without moving data. On HDFS, we'd have to export data to another system."

---

## Performance Comparison

| Metric | Hadoop Cluster | AWS Glue + S3 |
|---|---|---|
| Startup Time | ~5-10 min (cluster boot) | ~2-3 min (serverless) |
| Idle Cost | High (cluster running 24/7) | $0 (pay per use) |
| 100M row ETL | ~45-90 min | ~15-30 min (auto-scaled) |
| Schema discovery | Manual `MSCK REPAIR TABLE` | Auto via Glue Crawler |
| Monitoring | Ambari / Ganglia | CloudWatch (integrated) |
| Data durability | 3× replication on HDFS | 11 nines (S3 built-in) |
