#!/usr/bin/env bash
set -euo pipefail

AWS_REGION="${AWS_REGION:-us-east-1}"
STACK_NAME="${STACK_NAME:-bms-realtime-pipeline}"
PROJECT_NAME="${PROJECT_NAME:-bms-realtime}"
ENABLE_PUBLIC_REDSHIFT_ACCESS="${ENABLE_PUBLIC_REDSHIFT_ACCESS:-true}"
ALLOWED_CLIENT_IP_CIDR="${ALLOWED_CLIENT_IP_CIDR:-0.0.0.0/0}"

echo "Deploying stack: $STACK_NAME"
echo "Region: $AWS_REGION"
echo "ProjectName: $PROJECT_NAME"
echo "EnablePublicRedshiftAccess: $ENABLE_PUBLIC_REDSHIFT_ACCESS"
echo "AllowedClientIpCidr: $ALLOWED_CLIENT_IP_CIDR"

aws cloudformation deploy \
  --region "$AWS_REGION" \
  --stack-name "$STACK_NAME" \
  --template-file cloudformation/bookmyshow_realtime_pipeline.yaml \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    ProjectName="$PROJECT_NAME" \
    EnablePublicRedshiftAccess="$ENABLE_PUBLIC_REDSHIFT_ACCESS" \
    AllowedClientIpCidr="$ALLOWED_CLIENT_IP_CIDR"

REDSHIFT_WORKGROUP_NAME="$(
  aws cloudformation describe-stacks \
    --region "$AWS_REGION" \
    --stack-name "$STACK_NAME" \
    --query "Stacks[0].Outputs[?OutputKey=='RedshiftWorkgroupName'].OutputValue | [0]" \
    --output text
)"

# CloudFormation records the public-access parameter, but some existing
# Redshift Serverless workgroups do not flip the live public flag on stack
# update. This direct update keeps rerunning this script idempotent.
if [[ "$ENABLE_PUBLIC_REDSHIFT_ACCESS" == "true" ]]; then
  aws redshift-serverless update-workgroup \
    --region "$AWS_REGION" \
    --workgroup-name "$REDSHIFT_WORKGROUP_NAME" \
    --publicly-accessible >/dev/null
else
  aws redshift-serverless update-workgroup \
    --region "$AWS_REGION" \
    --workgroup-name "$REDSHIFT_WORKGROUP_NAME" \
    --no-publicly-accessible >/dev/null
fi

echo "Waiting for Redshift workgroup to become AVAILABLE..."
for _ in {1..30}; do
  WORKGROUP_STATUS="$(
    aws redshift-serverless get-workgroup \
      --region "$AWS_REGION" \
      --workgroup-name "$REDSHIFT_WORKGROUP_NAME" \
      --query "workgroup.status" \
      --output text
  )"

  if [[ "$WORKGROUP_STATUS" == "AVAILABLE" ]]; then
    break
  fi

  sleep 10
done

aws redshift-serverless get-workgroup \
  --region "$AWS_REGION" \
  --workgroup-name "$REDSHIFT_WORKGROUP_NAME" \
  --query "workgroup.{status:status,publiclyAccessible:publiclyAccessible,endpoint:endpoint.address}" \
  --output table

aws cloudformation describe-stacks \
  --region "$AWS_REGION" \
  --stack-name "$STACK_NAME" \
  --query "Stacks[0].Outputs" \
  --output table
