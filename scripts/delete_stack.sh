#!/usr/bin/env bash
set -euo pipefail

AWS_REGION="${AWS_REGION:-us-east-1}"
STACK_NAME="${STACK_NAME:-bms-realtime-pipeline}"

GLUE_JOB_NAME="$(
  aws cloudformation describe-stacks \
    --region "$AWS_REGION" \
    --stack-name "$STACK_NAME" \
    --query "Stacks[0].Outputs[?OutputKey=='GlueJobName'].OutputValue | [0]" \
    --output text 2>/dev/null || true
)"

if [[ -n "$GLUE_JOB_NAME" && "$GLUE_JOB_NAME" != "None" ]]; then
  RUN_IDS="$(
    aws glue get-job-runs \
      --region "$AWS_REGION" \
      --job-name "$GLUE_JOB_NAME" \
      --query "JobRuns[?JobRunState=='RUNNING'].Id" \
      --output text 2>/dev/null || true
  )"

  for RUN_ID in $RUN_IDS; do
    aws glue stop-job-run \
      --region "$AWS_REGION" \
      --job-name "$GLUE_JOB_NAME" \
      --run-id "$RUN_ID"
  done
fi

aws cloudformation delete-stack \
  --region "$AWS_REGION" \
  --stack-name "$STACK_NAME"

echo "Delete requested for CloudFormation stack: $STACK_NAME"

