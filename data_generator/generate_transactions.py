"""
FinFlow - Financial Transaction Data Generator
================================================
Generates realistic synthetic financial transaction data for the AWS pipeline demo.

Usage:
    python generate_transactions.py --rows 100000 --output ./output/
    python generate_transactions.py --rows 1000000 --output s3://your-bucket/raw/transactions/

Author: [Your Name] | AWS Data Engineering Portfolio Project
"""

import argparse
import csv
import os
import random
import uuid
from datetime import datetime, timedelta

import boto3

# ─── Constants ────────────────────────────────────────────────────────────────

MERCHANTS = [
    "Amazon", "Walmart", "Target", "Costco", "Best Buy",
    "Netflix", "Spotify", "Uber", "Lyft", "DoorDash",
    "Airbnb", "Delta Airlines", "Marriott Hotels", "Shell Gas", "BP Gas",
    "Whole Foods", "Trader Joe's", "CVS Pharmacy", "Walgreens", "Home Depot",
    "Apple Store", "Google Play", "Steam", "PayPal", "Venmo",
    "Chase ATM", "Bank of America ATM", "Wells Fargo", "Citibank", "USAA",
]

CATEGORIES = {
    "Amazon": "E-commerce",
    "Walmart": "Retail",
    "Target": "Retail",
    "Costco": "Retail",
    "Best Buy": "Electronics",
    "Netflix": "Streaming",
    "Spotify": "Streaming",
    "Uber": "Transport",
    "Lyft": "Transport",
    "DoorDash": "Food Delivery",
    "Airbnb": "Travel",
    "Delta Airlines": "Travel",
    "Marriott Hotels": "Travel",
    "Shell Gas": "Fuel",
    "BP Gas": "Fuel",
    "Whole Foods": "Grocery",
    "Trader Joe's": "Grocery",
    "CVS Pharmacy": "Healthcare",
    "Walgreens": "Healthcare",
    "Home Depot": "Home Improvement",
    "Apple Store": "Electronics",
    "Google Play": "Digital Services",
    "Steam": "Gaming",
    "PayPal": "Financial Services",
    "Venmo": "Financial Services",
    "Chase ATM": "ATM Withdrawal",
    "Bank of America ATM": "ATM Withdrawal",
    "Wells Fargo": "Banking",
    "Citibank": "Banking",
    "USAA": "Insurance",
}

COUNTRIES = ["US"] * 70 + ["UK", "CA", "DE", "FR", "AU", "JP", "SG", "IN", "BR"] * 3 + \
            ["CN", "MX", "RU", "NG", "ZA"]  # some unusual countries for fraud signals

PAYMENT_METHODS = ["credit_card", "debit_card", "bank_transfer", "digital_wallet", "crypto"]
CARD_TYPES = ["Visa", "Mastercard", "Amex", "Discover"]
STATUSES = ["completed"] * 90 + ["pending"] * 5 + ["failed"] * 3 + ["reversed"] * 2

# Amount distributions by category
AMOUNT_RANGES = {
    "E-commerce": (5.0, 500.0),
    "Retail": (10.0, 300.0),
    "Electronics": (50.0, 2000.0),
    "Streaming": (5.0, 20.0),
    "Transport": (5.0, 80.0),
    "Food Delivery": (15.0, 100.0),
    "Travel": (100.0, 5000.0),
    "Fuel": (30.0, 120.0),
    "Grocery": (20.0, 200.0),
    "Healthcare": (10.0, 300.0),
    "Home Improvement": (20.0, 500.0),
    "Digital Services": (1.0, 50.0),
    "Gaming": (1.0, 100.0),
    "Financial Services": (10.0, 1000.0),
    "ATM Withdrawal": (20.0, 500.0),
    "Banking": (50.0, 5000.0),
    "Insurance": (50.0, 500.0),
}


# ─── Helper Functions ─────────────────────────────────────────────────────────

def random_user_id(num_users: int = 5000) -> str:
    """Returns a user ID — uses fixed pool to simulate real user behavior."""
    return f"USER_{random.randint(1, num_users):06d}"


def random_amount(category: str, is_fraud: bool) -> float:
    """Generates a realistic transaction amount; fraud transactions are unusually large."""
    low, high = AMOUNT_RANGES.get(category, (10.0, 1000.0))
    if is_fraud:
        # Fraud: either very small (testing) or very large (money mule)
        fraud_type = random.choice(["micro", "macro"])
        if fraud_type == "micro":
            return round(random.uniform(0.01, 1.50), 2)
        else:
            return round(random.uniform(high * 3, high * 10), 2)
    return round(random.uniform(low, high), 2)


def is_fraud_transaction(country: str, amount: float, category: str) -> tuple[bool, str]:
    """
    Rule-based fraud labeling for training/demo purposes.
    Returns (is_fraud, fraud_reason).
    """
    fraud_countries = ["NG", "RU", "CN"]
    high_risk_categories = ["ATM Withdrawal", "Financial Services"]

    # Base fraud rate: ~2%
    if random.random() > 0.02:
        return False, ""

    reasons = []
    if country in fraud_countries:
        reasons.append("high_risk_country")
    if amount > 5000:
        reasons.append("high_amount")
    if category in high_risk_categories and amount > 1000:
        reasons.append("high_risk_category_high_amount")

    if not reasons:
        reasons.append("anomalous_pattern")

    return True, "|".join(reasons)


def generate_transaction(tx_date: datetime) -> dict:
    """Generates a single transaction record."""
    merchant = random.choice(MERCHANTS)
    category = CATEGORIES[merchant]
    country = random.choice(COUNTRIES)

    # Determine fraud first so amount can reflect it
    # We'll do a quick check — refine after generating amount
    is_fraud_base = random.random() < 0.02
    amount = random_amount(category, is_fraud_base)
    is_fraud, fraud_reason = is_fraud_transaction(country, amount, category)

    return {
        "transaction_id": str(uuid.uuid4()),
        "timestamp": tx_date.strftime("%Y-%m-%d %H:%M:%S"),
        "date": tx_date.strftime("%Y-%m-%d"),
        "hour": tx_date.hour,
        "user_id": random_user_id(),
        "merchant_name": merchant,
        "category": category,
        "amount": amount,
        "currency": "USD",
        "payment_method": random.choice(PAYMENT_METHODS),
        "card_type": random.choice(CARD_TYPES) if random.random() > 0.3 else "",
        "country": country,
        "status": random.choice(STATUSES),
        "is_fraud": int(is_fraud),
        "fraud_reason": fraud_reason,
        "device_type": random.choice(["mobile", "desktop", "tablet", "POS"]),
        "ip_address": f"{random.randint(1,255)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(0,255)}",
        "session_id": str(uuid.uuid4())[:8],
    }


# ─── Main Generator ───────────────────────────────────────────────────────────

def generate_dataset(
    num_rows: int,
    output_path: str,
    start_date: str = "2024-01-01",
    end_date: str = "2024-12-31",
    batch_size: int = 50000,
    use_s3: bool = False,
    s3_bucket: str = None,
    s3_prefix: str = "raw/transactions",
) -> None:
    """
    Generates synthetic financial transactions and writes to CSV.

    Args:
        num_rows: Total number of rows to generate.
        output_path: Local directory path or S3 URI (s3://bucket/prefix/).
        start_date: Start date for transactions (YYYY-MM-DD).
        end_date: End date for transactions (YYYY-MM-DD).
        batch_size: Rows per output file (simulates daily partitions).
        use_s3: If True, upload to S3.
        s3_bucket: S3 bucket name (if use_s3=True).
        s3_prefix: S3 prefix path.
    """
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    date_range = (end_dt - start_dt).days

    print(f"🚀 FinFlow Data Generator")
    print(f"   Rows: {num_rows:,}")
    print(f"   Date Range: {start_date} → {end_date}")
    print(f"   Batch Size: {batch_size:,} rows/file")
    print(f"   Output: {output_path}")
    print()

    if not use_s3:
        os.makedirs(output_path, exist_ok=True)

    fieldnames = [
        "transaction_id", "timestamp", "date", "hour", "user_id",
        "merchant_name", "category", "amount", "currency",
        "payment_method", "card_type", "country", "status",
        "is_fraud", "fraud_reason", "device_type", "ip_address", "session_id"
    ]

    batch_num = 0
    rows_written = 0

    while rows_written < num_rows:
        batch_num += 1
        rows_this_batch = min(batch_size, num_rows - rows_written)

        # Generate partition date for this batch (simulates daily data landing)
        partition_date = (start_dt + timedelta(days=(batch_num - 1) % date_range)).strftime("%Y-%m-%d")
        filename = f"transactions_{partition_date}_batch{batch_num:04d}.csv"

        rows = []
        for _ in range(rows_this_batch):
            # Random timestamp within partition date
            random_seconds = random.randint(0, 86399)
            tx_time = datetime.strptime(partition_date, "%Y-%m-%d") + timedelta(seconds=random_seconds)
            rows.append(generate_transaction(tx_time))

        # Write to local file
        local_file = os.path.join(output_path if not use_s3 else "/tmp", filename)
        with open(local_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        # Upload to S3 if needed
        if use_s3:
            s3_client = boto3.client("s3")
            s3_key = f"{s3_prefix}/year={partition_date[:4]}/month={partition_date[5:7]}/day={partition_date[8:10]}/{filename}"
            s3_client.upload_file(local_file, s3_bucket, s3_key)
            print(f"  ✅ Uploaded s3://{s3_bucket}/{s3_key}")
        else:
            print(f"  ✅ Wrote {rows_this_batch:,} rows → {filename}")

        rows_written += rows_this_batch

    print(f"\n🎉 Done! Generated {rows_written:,} rows across {batch_num} files.")


# ─── CLI Entry Point ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="FinFlow: Synthetic Financial Transaction Data Generator"
    )
    parser.add_argument("--rows", type=int, default=100_000, help="Number of rows to generate")
    parser.add_argument("--output", type=str, default="./output", help="Output directory path")
    parser.add_argument("--start-date", type=str, default="2024-01-01")
    parser.add_argument("--end-date", type=str, default="2024-12-31")
    parser.add_argument("--batch-size", type=int, default=50_000, help="Rows per output file")
    parser.add_argument("--s3-bucket", type=str, default=None, help="S3 bucket name for upload")
    parser.add_argument("--s3-prefix", type=str, default="raw/transactions")

    args = parser.parse_args()
    use_s3 = args.s3_bucket is not None

    generate_dataset(
        num_rows=args.rows,
        output_path=args.output,
        start_date=args.start_date,
        end_date=args.end_date,
        batch_size=args.batch_size,
        use_s3=use_s3,
        s3_bucket=args.s3_bucket,
        s3_prefix=args.s3_prefix,
    )
