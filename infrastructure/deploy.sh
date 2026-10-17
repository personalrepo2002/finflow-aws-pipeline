#!/usr/bin/env bash
# ============================================================
# FinFlow - AWS Deployment Script
# ============================================================
# Usage: ./deploy.sh [dev|staging|prod] [aws-profile]
# Example: ./deploy.sh dev my-aws-profile
# ============================================================

set -euo pipefail

ENVIRONMENT=${1:-dev}
AWS_PROFILE=${2:-default}
PROJECT_NAME="finflow"
AWS_REGION="us-east-1"
STACK_NAME="${PROJECT_NAME}-pipeline-${ENVIRONMENT}"

echo ""
echo "╔══════════════════════════════════════════════╗"
echo "║       FinFlow Pipeline Deployment            ║"
echo "╠══════════════════════════════════════════════╣"
echo "║  Environment : ${ENVIRONMENT}"
echo "║  AWS Profile : ${AWS_PROFILE}"
echo "║  Region      : ${AWS_REGION}"
echo "╚══════════════════════════════════════════════╝"
echo ""

export AWS_PROFILE="${AWS_PROFILE}"

# ── Step 1: Get AWS Account ID ────────────────────────────────────────────────
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
DATA_LAKE_BUCKET="${PROJECT_NAME}-data-lake-${ACCOUNT_ID}-${ENVIRONMENT}"
echo "✅ AWS Account: ${ACCOUNT_ID}"

# ── Step 2: Deploy CloudFormation Stack ───────────────────────────────────────
echo ""
echo "📦 Deploying CloudFormation stack: ${STACK_NAME}..."

aws cloudformation deploy \
  --template-file infrastructure/cloudformation.yaml \
  --stack-name "${STACK_NAME}" \
  --parameter-overrides \
      ProjectName="${PROJECT_NAME}" \
      Environment="${ENVIRONMENT}" \
      AlertEmail="your-email@example.com" \
  --capabilities CAPABILITY_NAMED_IAM \
  --region "${AWS_REGION}"

echo "✅ CloudFormation stack deployed"

# ── Step 3: Upload Glue Scripts to S3 ────────────────────────────────────────
echo ""
echo "📤 Uploading Glue scripts to S3..."

aws s3 cp glue_jobs/01_raw_to_processed.py  "s3://${DATA_LAKE_BUCKET}/scripts/"
aws s3 cp glue_jobs/02_fraud_detection.py   "s3://${DATA_LAKE_BUCKET}/scripts/"
aws s3 cp glue_jobs/03_load_to_redshift.py  "s3://${DATA_LAKE_BUCKET}/scripts/"
echo "✅ Glue scripts uploaded"

# ── Step 4: Upload Step Functions Definition ──────────────────────────────────
echo ""
echo "📤 Uploading Step Functions definition..."

# Replace placeholder ACCOUNT_ID in definition
sed "s/ACCOUNT_ID/${ACCOUNT_ID}/g" step_functions/pipeline_definition.json > /tmp/pipeline_definition_resolved.json
aws s3 cp /tmp/pipeline_definition_resolved.json "s3://${DATA_LAKE_BUCKET}/step_functions/pipeline_definition.json"
echo "✅ Step Functions definition uploaded"

# ── Step 5: Create Athena Workgroup ──────────────────────────────────────────
echo ""
echo "⚙️  Configuring Athena workgroup..."
ATHENA_BUCKET="${PROJECT_NAME}-athena-results-${ACCOUNT_ID}-${ENVIRONMENT}"

aws athena create-work-group \
  --name "${PROJECT_NAME}-workgroup" \
  --configuration "ResultConfiguration={OutputLocation=s3://${ATHENA_BUCKET}/},EnforceWorkGroupConfiguration=true" \
  --description "FinFlow Athena Workgroup" \
  --region "${AWS_REGION}" 2>/dev/null || echo "  (Workgroup already exists)"
echo "✅ Athena workgroup ready"

# ── Step 6: Generate Sample Data ─────────────────────────────────────────────
echo ""
read -p "🎲 Generate sample data and upload to S3? (y/N): " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Generating 500,000 sample transactions..."
    pip install -q boto3
    python data_generator/generate_transactions.py \
      --rows 500000 \
      --s3-bucket "${DATA_LAKE_BUCKET}" \
      --s3-prefix "raw/transactions"
    echo "✅ Sample data uploaded to S3"
fi

# ── Step 7: Create Athena Tables ─────────────────────────────────────────────
echo ""
echo "🗄️  Creating Athena external tables..."

# Replace placeholder bucket name
sed "s/YOUR-BUCKET/${DATA_LAKE_BUCKET}/g" athena_queries/01_create_tables.sql > /tmp/create_tables_resolved.sql

# Run each SQL statement (simplified — use Athena API in production)
echo "  ⚠️  Please run athena_queries/01_create_tables.sql manually in Athena console"
echo "      (or use aws athena start-query-execution)"

# ── Step 8: Print Summary ──────────────────────────────────────────────────────
echo ""
echo "════════════════════════════════════════════════════"
echo "  🎉 FinFlow Deployment Complete!"
echo "════════════════════════════════════════════════════"
echo ""
echo "  📊 Key Resources:"
echo "     Data Lake   : s3://${DATA_LAKE_BUCKET}"
echo "     Glue DB     : ${PROJECT_NAME}_db"
echo "     Stack       : ${STACK_NAME}"
echo ""
echo "  ▶️  To run the pipeline manually:"
echo "     aws stepfunctions start-execution \\"
echo "       --state-machine-arn \$(aws cloudformation describe-stacks \\"
echo "         --stack-name ${STACK_NAME} \\"
echo "         --query 'Stacks[0].Outputs[?OutputKey==\`StateMachineArn\`].OutputValue' \\"
echo "         --output text) \\"
echo "       --name 'manual-run-\$(date +%Y%m%d-%H%M%S)'"
echo ""
