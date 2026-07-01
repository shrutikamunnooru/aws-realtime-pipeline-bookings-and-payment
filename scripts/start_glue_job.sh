#!/usr/bin/env bash
set -euo pipefail

AWS_REGION="${AWS_REGION:-us-east-1}"
STACK_NAME="${STACK_NAME:-bms-realtime-pipeline}"

GLUE_JOB_NAME="$(
  aws cloudformation describe-stacks \
    --region "$AWS_REGION" \
    --stack-name "$STACK_NAME" \
    --query "Stacks[0].Outputs[?OutputKey=='GlueJobName'].OutputValue | [0]" \
    --output text
)"

aws glue start-job-run \
  --region "$AWS_REGION" \
  --job-name "$GLUE_JOB_NAME"

