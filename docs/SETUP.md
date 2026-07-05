# Full Deployment Walkthrough

Detailed, step-by-step version of the "Deployment Overview" in the main README — useful if you want to reproduce this project exactly from a blank AWS account.

## 1. Configure AWS CLI

```bash
aws configure
aws sts get-caller-identity
```

## 2. Move Into the Project Folder

```bash
cd bookmyshow-aws-realtime-pipeline
```

## 3. Deploy the CloudFormation Stack

Recommended one-command deployment:

```bash
aws cloudformation deploy \
  --region us-east-1 \
  --stack-name bms-realtime-pipeline \
  --template-file cloudformation/bookmyshow_realtime_pipeline.yaml \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides ProjectName=bms-realtime
```

For local development tools or BI clients that need to connect to Redshift directly, use the helper script instead — it opens Redshift publicly on port `5439`:

```bash
export AWS_REGION=us-east-1
export STACK_NAME=bms-realtime-pipeline
export PROJECT_NAME=bms-realtime

bash scripts/deploy_stack.sh
```

By default, this allows `0.0.0.0/0` for demo convenience — use this only for demos, and restrict `ALLOWED_CLIENT_IP_CIDR` to known static IPs for anything beyond that. The script enables demo-friendly public Redshift access and force-updates the Redshift Serverless workgroup public flag after CloudFormation finishes.

Deployment may take several minutes because Redshift Serverless and VPC endpoints are provisioned.

**Known issues and fixes:**

- If an earlier deployment failed while creating `RedshiftNamespace`, delete the failed stack first and redeploy — CloudFormation will generate a new Redshift password using corrected character rules.
- If an earlier deployment failed while creating `GlueStreamingJob`, delete the failed stack and redeploy with the latest template (the Glue JDBC URL uses Redshift Serverless port `5439` as a string so CloudFormation can create the job arguments correctly).
- If the Glue job fails with `Column 'event_type' does not exist` or `Column 'data' does not exist`, upload the latest Glue script again — the job uses Spark's native Kinesis streaming reader, so the script receives the raw Kinesis payload and parses the JSON itself.
- The Redshift table is created by the Glue job if it doesn't already exist. Money fields use `DOUBLE PRECISION` to match the Spark `double` output from the streaming job.
- If a Redshift connectivity test resolves the endpoint to `10.70.x.x` and then times out, Redshift is private — rerun `./scripts/deploy_stack.sh`, which sets `EnablePublicRedshiftAccess=true`, `AllowedClientIpCidr=0.0.0.0/0`, and force-updates the workgroup public flag directly.
- A stack stuck in `ROLLBACK_COMPLETE` must be deleted and recreated — CloudFormation won't update a stack in that state.

## 4. Review Stack Outputs

```bash
aws cloudformation describe-stacks \
  --region us-east-1 \
  --stack-name bms-realtime-pipeline \
  --query "Stacks[0].Outputs" \
  --output table
```

Important outputs: `BookingStreamName`, `PaymentStreamName`, `DlqQueueUrl`, `GlueAssetsBucketName`, `GlueJobName`, `RedshiftWorkgroupName`, `RedshiftDatabaseName`, `RedshiftAdminSecretArn`.

### 4a. Fetch Redshift Username and Password

```bash
export AWS_REGION=us-east-1
export STACK_NAME=bms-realtime-pipeline

export REDSHIFT_SECRET_ARN=$(aws cloudformation describe-stacks \
  --region "$AWS_REGION" \
  --stack-name "$STACK_NAME" \
  --query "Stacks[0].Outputs[?OutputKey=='RedshiftAdminSecretArn'].OutputValue | [0]" \
  --output text)

aws secretsmanager get-secret-value \
  --region "$AWS_REGION" \
  --secret-id "$REDSHIFT_SECRET_ARN" \
  --query SecretString \
  --output text
```

Export username/password for local tools or Python tests:

```bash
export REDSHIFT_USER=$(aws secretsmanager get-secret-value \
  --region "$AWS_REGION" --secret-id "$REDSHIFT_SECRET_ARN" \
  --query SecretString --output text | python3 -c 'import json,sys; print(json.load(sys.stdin)["username"])')

export REDSHIFT_PASSWORD=$(aws secretsmanager get-secret-value \
  --region "$AWS_REGION" --secret-id "$REDSHIFT_SECRET_ARN" \
  --query SecretString --output text | python3 -c 'import json,sys; print(json.load(sys.stdin)["password"])')
```

For Query Editor v2 or a local Redshift client:

```text
Host: bms-realtime-wg.<your-account-id>.us-east-1.redshift-serverless.amazonaws.com
Port: 5439
Database: bmsdev
Username: value of REDSHIFT_USER
Password: value of REDSHIFT_PASSWORD
SSL: require / enable
```

## 5. Upload the Glue Script

CloudFormation creates the Glue job and S3 bucket, but the local script still needs uploading:

```bash
bash scripts/upload_glue_job.sh
```

Equivalent manual command:

```bash
GLUE_ASSETS_BUCKET=$(aws cloudformation describe-stacks \
  --region us-east-1 --stack-name bms-realtime-pipeline \
  --query "Stacks[0].Outputs[?OutputKey=='GlueAssetsBucketName'].OutputValue | [0]" \
  --output text)

aws s3 cp glue_jobs/bookmyshow_streaming_etl.py \
  "s3://${GLUE_ASSETS_BUCKET}/scripts/bookmyshow_streaming_etl.py" \
  --region us-east-1
```

## 6. Start the Glue Streaming Job

```bash
bash scripts/start_glue_job.sh
```

Equivalent manual command:

```bash
GLUE_JOB_NAME=$(aws cloudformation describe-stacks \
  --region us-east-1 --stack-name bms-realtime-pipeline \
  --query "Stacks[0].Outputs[?OutputKey=='GlueJobName'].OutputValue | [0]" \
  --output text)

aws glue start-job-run --region us-east-1 --job-name "$GLUE_JOB_NAME"
```

The Glue job creates the Redshift schema/table automatically when it starts.

## 7. Run the Mock Producer

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-producer.txt
```

Export stream names from CloudFormation outputs:

```bash
export AWS_REGION=us-east-1
export BOOKING_STREAM_NAME=$(aws cloudformation describe-stacks \
  --region "$AWS_REGION" --stack-name bms-realtime-pipeline \
  --query "Stacks[0].Outputs[?OutputKey=='BookingStreamName'].OutputValue | [0]" \
  --output text)

export PAYMENT_STREAM_NAME=$(aws cloudformation describe-stacks \
  --region "$AWS_REGION" --stack-name bms-realtime-pipeline \
  --query "Stacks[0].Outputs[?OutputKey=='PaymentStreamName'].OutputValue | [0]" \
  --output text)
```

Run a limited test:

```bash
python producers/mock_bms_event_producer.py --event-count 100 --interval-seconds 0.5
```

Run continuously:

```bash
python producers/mock_bms_event_producer.py
```

Generate more unmatched and failed-payment test cases:

```bash
python producers/mock_bms_event_producer.py \
  --event-count 500 --interval-seconds 0.25 \
  --unmatched-payment-rate 0.10 --out-of-window-payment-rate 0.05 --failed-payment-rate 0.08
```

## Validation Detail

### Check Glue Logs

Open CloudWatch Logs for the Glue job and confirm: the job starts successfully, Redshift schema/table initialization finishes, Kinesis micro-batches are running, Redshift writes are happening, and SQS DLQ writes are happening for invalid or unmatched records.

### Query Redshift

You can also connect from a local Redshift client (e.g. `psql`) using the stack outputs and Secrets Manager credentials pulled earlier:

```bash
psql "host=<RedshiftWorkgroupEndpoint> port=5439 dbname=bmsdev user=awsuser sslmode=require"
```

The workgroup endpoint isn't a direct CloudFormation output — get it from the Redshift Serverless console (Workgroup configuration) or via `aws redshift-serverless get-workgroup`. This only works if `EnablePublicRedshiftAccess` was set to `true` at deploy time; by default Redshift is private and only reachable from inside the VPC (e.g. through the Glue job itself, or Query Editor v2 in the console).

### Check SQS DLQ

```bash
DLQ_QUEUE_URL=$(aws cloudformation describe-stacks \
  --region us-east-1 --stack-name bms-realtime-pipeline \
  --query "Stacks[0].Outputs[?OutputKey=='DlqQueueUrl'].OutputValue | [0]" \
  --output text)

aws sqs receive-message \
  --region us-east-1 --queue-url "$DLQ_QUEUE_URL" \
  --max-number-of-messages 10 --wait-time-seconds 5
```
