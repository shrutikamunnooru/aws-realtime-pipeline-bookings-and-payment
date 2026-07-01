"""
AWS Glue Streaming ETL job for the BookMyShow real-time pipeline.

Flow of this job:
1. Read booking and payment JSON events from two Amazon Kinesis Data Streams.
2. Parse and normalize both event streams into typed Spark DataFrames.
3. Validate required fields and send invalid events to an SQS DLQ.
4. Perform a stateful stream-stream join using Spark watermarks.
5. Write matched booking-payment transactions to Amazon Redshift.
6. Send bookings that do not receive a matching payment within the join window to SQS.

This file is intentionally written in a sequential, readable style for learning and project
demonstration. The few helper functions exist only to keep repeated write logic small.
"""

from __future__ import annotations

import json
import time
from typing import Iterable

import boto3
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, Row
from pyspark.sql.functions import (
    col,
    concat_ws,
    expr,
    from_json,
    lit,
    struct,
    to_json,
    to_timestamp,
    when,
)
from pyspark.sql.types import ArrayType, DoubleType, IntegerType, StringType, StructField, StructType


args = getResolvedOptions(
    __import__("sys").argv,
    [
        "JOB_NAME",
        "AWS_REGION",
        "BOOKING_STREAM_ARN",
        "PAYMENT_STREAM_ARN",
        "CHECKPOINT_S3_PATH",
        "DLQ_QUEUE_URL",
        "REDSHIFT_DATABASE",
        "REDSHIFT_TABLE",
        "REDSHIFT_TEMP_S3_DIR",
        "REDSHIFT_JDBC_URL",
        "REDSHIFT_SECRET_ARN",
        "REDSHIFT_COPY_ROLE_ARN",
        "REDSHIFT_WORKGROUP_NAME",
        "JOIN_WINDOW_MINUTES",
        "WATERMARK_DELAY_MINUTES",
        "STARTING_POSITION",
    ],
)


AWS_REGION = args["AWS_REGION"]
BOOKING_STREAM_ARN = args["BOOKING_STREAM_ARN"]
PAYMENT_STREAM_ARN = args["PAYMENT_STREAM_ARN"]
CHECKPOINT_S3_PATH = args["CHECKPOINT_S3_PATH"].rstrip("/")
DLQ_QUEUE_URL = args["DLQ_QUEUE_URL"]
REDSHIFT_DATABASE = args["REDSHIFT_DATABASE"]
REDSHIFT_TABLE = args["REDSHIFT_TABLE"]
REDSHIFT_TEMP_S3_DIR = args["REDSHIFT_TEMP_S3_DIR"]
REDSHIFT_JDBC_URL = args["REDSHIFT_JDBC_URL"]
REDSHIFT_SECRET_ARN = args["REDSHIFT_SECRET_ARN"]
REDSHIFT_COPY_ROLE_ARN = args["REDSHIFT_COPY_ROLE_ARN"]
REDSHIFT_WORKGROUP_NAME = args["REDSHIFT_WORKGROUP_NAME"]
JOIN_WINDOW_MINUTES = int(args["JOIN_WINDOW_MINUTES"])
WATERMARK_DELAY_MINUTES = int(args["WATERMARK_DELAY_MINUTES"])
STARTING_POSITION = args["STARTING_POSITION"]


CREATE_SCHEMA_SQL = "CREATE SCHEMA IF NOT EXISTS bms;"

CREATE_ENRICHED_TRANSACTIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS bms.enriched_transactions (
    booking_id             VARCHAR(64)   NOT NULL,
    booking_event_id       VARCHAR(64),
    payment_event_id       VARCHAR(64),
    payment_id             VARCHAR(64),
    user_id                VARCHAR(32),
    booking_ts             TIMESTAMP,
    payment_ts             TIMESTAMP,
    show_ts                TIMESTAMP,
    show_id                VARCHAR(96),
    movie_id               VARCHAR(32),
    movie_name             VARCHAR(128),
    genre                  VARCHAR(64),
    language               VARCHAR(32),
    certificate            VARCHAR(16),
    city                   VARCHAR(64),
    venue_id               VARCHAR(32),
    venue_name             VARCHAR(128),
    screen_name            VARCHAR(64),
    seats                  VARCHAR(256),
    seat_count             INTEGER,
    ticket_category        VARCHAR(32),
    ticket_price           DOUBLE PRECISION,
    convenience_fee        DOUBLE PRECISION,
    taxes                  DOUBLE PRECISION,
    discount_amount        DOUBLE PRECISION,
    booking_amount         DOUBLE PRECISION,
    payment_amount         DOUBLE PRECISION,
    payment_method         VARCHAR(32),
    payment_provider       VARCHAR(32),
    payment_status         VARCHAR(32),
    bank_name              VARCHAR(64),
    upi_app                VARCHAR(64),
    failure_reason         VARCHAR(64),
    channel                VARCHAR(32),
    device_type            VARCHAR(32),
    currency               VARCHAR(8)
)
DISTSTYLE AUTO
SORTKEY (booking_ts);
"""


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


booking_schema = StructType(
    [
        StructField("event_type", StringType()),
        StructField("event_id", StringType()),
        StructField("booking_id", StringType()),
        StructField("user_id", StringType()),
        StructField("booking_ts", StringType()),
        StructField("show_id", StringType()),
        StructField("movie_id", StringType()),
        StructField("movie_name", StringType()),
        StructField("genre", StringType()),
        StructField("language", StringType()),
        StructField("certificate", StringType()),
        StructField("city", StringType()),
        StructField("venue_id", StringType()),
        StructField("venue_name", StringType()),
        StructField("screen_name", StringType()),
        StructField("show_ts", StringType()),
        StructField("seats", ArrayType(StringType())),
        StructField("seat_count", IntegerType()),
        StructField("ticket_category", StringType()),
        StructField("ticket_price", DoubleType()),
        StructField("convenience_fee", DoubleType()),
        StructField("taxes", DoubleType()),
        StructField("discount_amount", DoubleType()),
        StructField("total_amount", DoubleType()),
        StructField("currency", StringType()),
        StructField("channel", StringType()),
        StructField("device_type", StringType()),
        StructField("booking_status", StringType()),
    ]
)


payment_schema = StructType(
    [
        StructField("event_type", StringType()),
        StructField("event_id", StringType()),
        StructField("booking_id", StringType()),
        StructField("payment_id", StringType()),
        StructField("payment_ts", StringType()),
        StructField("payment_method", StringType()),
        StructField("payment_provider", StringType()),
        StructField("payment_status", StringType()),
        StructField("amount", DoubleType()),
        StructField("currency", StringType()),
        StructField("bank_name", StringType()),
        StructField("upi_app", StringType()),
        StructField("failure_reason", StringType()),
    ]
)


def get_redshift_credentials() -> tuple[str, str]:
    """Read the stack-created Redshift username and password from Secrets Manager."""
    secrets_client = boto3.client("secretsmanager", region_name=AWS_REGION)
    secret_value = secrets_client.get_secret_value(SecretId=REDSHIFT_SECRET_ARN)
    secret_json = json.loads(secret_value["SecretString"])
    return secret_json["username"], secret_json["password"]


REDSHIFT_USERNAME, REDSHIFT_PASSWORD = get_redshift_credentials()


def wait_for_redshift_statement(statement_id: str) -> None:
    """Wait until one Redshift Data API statement finishes or fails."""
    redshift_data = boto3.client("redshift-data", region_name=AWS_REGION)

    while True:
        statement = redshift_data.describe_statement(Id=statement_id)
        status = statement["Status"]

        if status == "FINISHED":
            return

        if status in {"FAILED", "ABORTED"}:
            error_message = statement.get("Error", "No Redshift error message returned.")
            raise RuntimeError(f"Redshift statement {statement_id} ended with {status}: {error_message}")

        time.sleep(2)


def run_redshift_statement(sql: str) -> None:
    """Run one Redshift SQL statement through the Redshift Data API."""
    redshift_data = boto3.client("redshift-data", region_name=AWS_REGION)
    response = redshift_data.execute_statement(
        WorkgroupName=REDSHIFT_WORKGROUP_NAME,
        Database=REDSHIFT_DATABASE,
        SecretArn=REDSHIFT_SECRET_ARN,
        Sql=sql,
    )
    wait_for_redshift_statement(response["Id"])


def initialize_redshift_schema() -> None:
    """Create the Redshift schema and target table before streaming writes begin."""
    run_redshift_statement(CREATE_SCHEMA_SQL)
    run_redshift_statement(CREATE_ENRICHED_TRANSACTIONS_TABLE_SQL)


def stream_name_from_arn(stream_arn: str) -> str:
    """Extract the Kinesis stream name from an ARN like arn:aws:kinesis:region:acct:stream/name."""
    return stream_arn.rsplit("/", 1)[-1]


def read_kinesis_json_stream(stream_arn: str, schema: StructType, transformation_ctx: str) -> DataFrame:
    """Read a Kinesis stream and parse each record as JSON using the provided schema."""
    stream_name = stream_name_from_arn(stream_arn)

    raw_df = (
        spark.readStream.format("kinesis")
        .option("streamName", stream_name)
        .option("endpointUrl", f"https://kinesis.{AWS_REGION}.amazonaws.com")
        .option("startingPosition", STARTING_POSITION)
        .load()
    )

    # Spark's native Kinesis reader exposes the raw record payload in `data`.
    # We parse the producer's JSON payload exactly once using our explicit schema.
    parsed_df = raw_df.select(from_json(col("data").cast("string"), schema).alias("event"))
    return parsed_df.select("event.*")



def add_booking_validation_reason(df: DataFrame) -> DataFrame:
    """Attach a human-readable validation reason to invalid booking records."""
    return df.withColumn(
        "validation_error",
        when(col("booking_id").isNull(), lit("MISSING_BOOKING_ID"))
        .when(col("booking_event_time").isNull(), lit("INVALID_BOOKING_TIMESTAMP"))
        .when(col("total_amount").isNull() | (col("total_amount") <= 0), lit("INVALID_TOTAL_AMOUNT"))
        .when(col("seat_count").isNull() | (col("seat_count") <= 0), lit("INVALID_SEAT_COUNT"))
        .when(col("city").isNull(), lit("MISSING_CITY"))
        .otherwise(lit(None)),
    )


def add_payment_validation_reason(df: DataFrame) -> DataFrame:
    """Attach a human-readable validation reason to invalid payment records."""
    return df.withColumn(
        "validation_error",
        when(col("booking_id").isNull(), lit("MISSING_BOOKING_ID"))
        .when(col("payment_id").isNull(), lit("MISSING_PAYMENT_ID"))
        .when(col("payment_event_time").isNull(), lit("INVALID_PAYMENT_TIMESTAMP"))
        .when(col("amount").isNull() | (col("amount") <= 0), lit("INVALID_PAYMENT_AMOUNT"))
        .when(col("payment_status").isNull(), lit("MISSING_PAYMENT_STATUS"))
        .otherwise(lit(None)),
    )


def send_partition_to_sqs(rows: Iterable[Row], dlq_source: str) -> None:
    """Send rows from one Spark partition to SQS in batches of 10 messages."""
    sqs = boto3.client("sqs", region_name=AWS_REGION)
    entries = []

    for index, row in enumerate(rows):
        message = {
            "dlq_source": dlq_source,
            "payload": json.loads(row["payload"]),
        }
        entries.append({"Id": str(index % 10), "MessageBody": json.dumps(message, default=str)})

        if len(entries) == 10:
            sqs.send_message_batch(QueueUrl=DLQ_QUEUE_URL, Entries=entries)
            entries = []

    if entries:
        sqs.send_message_batch(QueueUrl=DLQ_QUEUE_URL, Entries=entries)


def write_batch_to_sqs(batch_df: DataFrame, batch_id: int, dlq_source: str) -> None:
    """Write a micro-batch DataFrame to SQS as DLQ messages."""
    if batch_df.rdd.isEmpty():
        return

    payload_df = batch_df.select(to_json(struct("*")).alias("payload"))
    payload_df.foreachPartition(lambda rows: send_partition_to_sqs(rows, dlq_source))


def write_matched_batch_to_redshift(batch_df: DataFrame, batch_id: int) -> None:
    """Write matched booking-payment records from one micro-batch to Redshift."""
    if batch_df.rdd.isEmpty():
        return

    dynamic_frame = DynamicFrame.fromDF(batch_df, glueContext, f"matched_batch_{batch_id}")
    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="redshift",
        connection_options={
            "url": REDSHIFT_JDBC_URL,
            "user": REDSHIFT_USERNAME,
            "password": REDSHIFT_PASSWORD,
            "dbtable": REDSHIFT_TABLE,
            "redshiftTmpDir": REDSHIFT_TEMP_S3_DIR,
            "aws_iam_role": REDSHIFT_COPY_ROLE_ARN,
        },
        transformation_ctx=f"redshift_write_{batch_id}",
    )


def process_joined_batch(batch_df: DataFrame, batch_id: int) -> None:
    """Split one joined micro-batch into matched Redshift rows and unmatched SQS rows."""
    if batch_df.rdd.isEmpty():
        return

    matched_df = batch_df.filter(col("payment_id").isNotNull())

    unmatched_df = (
        batch_df.filter(col("payment_id").isNull())
        .withColumn("dlq_reason", lit("PAYMENT_NOT_FOUND_WITHIN_JOIN_WINDOW"))
        .withColumn("join_window_minutes", lit(JOIN_WINDOW_MINUTES))
    )

    write_matched_batch_to_redshift(matched_df, batch_id)
    write_batch_to_sqs(unmatched_df, batch_id, "unmatched_booking_payment_join")


# ---------------------------------------------------------------------------
# Step 0: Create Redshift schema/table automatically.
#
# CloudFormation creates the Redshift Serverless workgroup. The Glue job creates
# the database objects when it starts, so there is no manual Query Editor step.
# ---------------------------------------------------------------------------
initialize_redshift_schema()


# ---------------------------------------------------------------------------
# Step 1: Read raw JSON events from both Kinesis streams.
# ---------------------------------------------------------------------------
booking_events = read_kinesis_json_stream(BOOKING_STREAM_ARN, booking_schema, "booking_events")
payment_events = read_kinesis_json_stream(PAYMENT_STREAM_ARN, payment_schema, "payment_events")


# ---------------------------------------------------------------------------
# Step 2: Normalize field types and create event-time columns used by Spark.
# ---------------------------------------------------------------------------
bookings_normalized = (
    booking_events.withColumn("booking_event_time", to_timestamp("booking_ts"))
    .withColumn("show_event_time", to_timestamp("show_ts"))
    .withColumn("seats_csv", concat_ws(",", col("seats")))
    .withColumn("ticket_price", col("ticket_price").cast("double"))
    .withColumn("convenience_fee", col("convenience_fee").cast("double"))
    .withColumn("taxes", col("taxes").cast("double"))
    .withColumn("discount_amount", col("discount_amount").cast("double"))
    .withColumn("total_amount", col("total_amount").cast("double"))
)

payments_normalized = payment_events.withColumn("payment_event_time", to_timestamp("payment_ts")).withColumn(
    "amount", col("amount").cast("double")
)


# ---------------------------------------------------------------------------
# Step 3: Validate both streams. Invalid records are sent to SQS
# ---------------------------------------------------------------------------
bookings_with_validation = add_booking_validation_reason(bookings_normalized)
payments_with_validation = add_payment_validation_reason(payments_normalized)

valid_bookings = bookings_with_validation.filter(col("validation_error").isNull()).drop("validation_error")
invalid_bookings = bookings_with_validation.filter(col("validation_error").isNotNull())

valid_payments = payments_with_validation.filter(col("validation_error").isNull()).drop("validation_error")
invalid_payments = payments_with_validation.filter(col("validation_error").isNotNull())


# ---------------------------------------------------------------------------
# Step 4: Stateful stream-stream join with watermarks.
#
# Spark keeps the temporary join state internally and uses the checkpoint path
# for recovery. No DynamoDB table is needed.
# ---------------------------------------------------------------------------
bookings_for_join = valid_bookings.withWatermark("booking_event_time", f"{WATERMARK_DELAY_MINUTES} minutes").alias(
    "booking"
)
payments_for_join = valid_payments.withWatermark("payment_event_time", f"{WATERMARK_DELAY_MINUTES} minutes").alias(
    "payment"
)

join_condition = expr(
    f"""
    booking.booking_id = payment.booking_id
    AND payment.payment_event_time >= booking.booking_event_time
    AND payment.payment_event_time <= booking.booking_event_time + interval {JOIN_WINDOW_MINUTES} minutes
    """
)

joined_transactions = bookings_for_join.join(payments_for_join, join_condition, "leftOuter")


# ---------------------------------------------------------------------------
# Step 5: Flatten the joined output into a Redshift-friendly shape.
# ---------------------------------------------------------------------------
enriched_transactions = joined_transactions.select(
    col("booking.booking_id").alias("booking_id"),
    col("booking.event_id").alias("booking_event_id"),
    col("payment.event_id").alias("payment_event_id"),
    col("payment.payment_id").alias("payment_id"),
    col("booking.user_id").alias("user_id"),
    col("booking.booking_event_time").alias("booking_ts"),
    col("payment.payment_event_time").alias("payment_ts"),
    col("booking.show_event_time").alias("show_ts"),
    col("booking.show_id").alias("show_id"),
    col("booking.movie_id").alias("movie_id"),
    col("booking.movie_name").alias("movie_name"),
    col("booking.genre").alias("genre"),
    col("booking.language").alias("language"),
    col("booking.certificate").alias("certificate"),
    col("booking.city").alias("city"),
    col("booking.venue_id").alias("venue_id"),
    col("booking.venue_name").alias("venue_name"),
    col("booking.screen_name").alias("screen_name"),
    col("booking.seats_csv").alias("seats"),
    col("booking.seat_count").alias("seat_count"),
    col("booking.ticket_category").alias("ticket_category"),
    col("booking.ticket_price").alias("ticket_price"),
    col("booking.convenience_fee").alias("convenience_fee"),
    col("booking.taxes").alias("taxes"),
    col("booking.discount_amount").alias("discount_amount"),
    col("booking.total_amount").alias("booking_amount"),
    col("payment.amount").alias("payment_amount"),
    col("payment.payment_method").alias("payment_method"),
    col("payment.payment_provider").alias("payment_provider"),
    col("payment.payment_status").alias("payment_status"),
    col("payment.bank_name").alias("bank_name"),
    col("payment.upi_app").alias("upi_app"),
    col("payment.failure_reason").alias("failure_reason"),
    col("booking.channel").alias("channel"),
    col("booking.device_type").alias("device_type"),
    col("booking.currency").alias("currency"),
)


# ---------------------------------------------------------------------------
# Step 6: Start the streaming writes.
# ---------------------------------------------------------------------------
joined_query = (
    enriched_transactions.writeStream.foreachBatch(process_joined_batch)
    .option("checkpointLocation", f"{CHECKPOINT_S3_PATH}/joined-transactions")
    .outputMode("append")
    .start()
)

invalid_booking_query = (
    invalid_bookings.writeStream.foreachBatch(
        lambda batch_df, batch_id: write_batch_to_sqs(batch_df, batch_id, "invalid_booking_event")
    )
    .option("checkpointLocation", f"{CHECKPOINT_S3_PATH}/invalid-bookings")
    .outputMode("append")
    .start()
)

invalid_payment_query = (
    invalid_payments.writeStream.foreachBatch(
        lambda batch_df, batch_id: write_batch_to_sqs(batch_df, batch_id, "invalid_payment_event")
    )
    .option("checkpointLocation", f"{CHECKPOINT_S3_PATH}/invalid-payments")
    .outputMode("append")
    .start()
)

spark.streams.awaitAnyTermination()
job.commit()
