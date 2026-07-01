#!/usr/bin/env bash
set -euo pipefail

AWS_REGION="${AWS_REGION:-us-east-1}"
STACK_NAME="${STACK_NAME:-bms-realtime-pipeline}"

GLUE_ASSETS_BUCKET="$(
  aws cloudformation describe-stacks \
    --region "$AWS_REGION" \
    --stack-name "$STACK_NAME" \
    --query "Stacks[0].Outputs[?OutputKey=='GlueAssetsBucketName'].OutputValue | [0]" \
    --output text
)"

aws s3 cp \
  glue_jobs/bookmyshow_streaming_etl.py \
  "s3://${GLUE_ASSETS_BUCKET}/scripts/bookmyshow_streaming_etl.py" \
  --region "$AWS_REGION"

echo "Uploaded Glue script to s3://${GLUE_ASSETS_BUCKET}/scripts/bookmyshow_streaming_etl.py"

