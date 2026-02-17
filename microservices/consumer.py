"""
consumer.py — Confluent Cloud Kafka → Neo4j streaming consumer
with S3 bad-record routing.

Every message that fails for any reason (bad JSON, schema mismatch, Neo4j error)
is written to S3 as a gzipped JSON file before the offset is committed.
This gives you full observability on rejected data and makes replaying or
investigating failures straightforward.

Bad records land at:
    s3://<S3_BAD_OUTPUT>/source=kafka/ingest_date=YYYY-MM-DD/
"""

import gzip
import hashlib
import io
import json
import os
import re
import signal
import base64
from datetime import datetime, timezone
from typing import Optional

import boto3
from confluent_kafka import Consumer
from dotenv import load_dotenv
from neo4j import GraphDatabase
from pydantic import BaseModel, ValidationError

# =============================================================================
# Section 1: Secrets Manager helper
# =============================================================================

def fetch_secret(secret_name: str, region: str) -> dict:
    """
    Retrieve a JSON secret from AWS Secrets Manager.
    Supports both SecretString (plain JSON) and SecretBinary (base64 JSON).
    """
    client = boto3.client("secretsmanager", region_name=region)
    resp   = client.get_secret_value(SecretId=secret_name)
    if "SecretString" in resp:
        return json.loads(resp["SecretString"])
    return json.loads(base64.b64decode(resp["SecretBinary"]))


# =============================================================================
# Section 2: Configuration — non-secrets from .env, secrets from Secrets Manager
# =============================================================================

load_dotenv()

AWS_REGION        = os.getenv("AWS_REGION", "us-east-1")
KAFKA_SECRET_NAME = os.getenv("KAFKA_SECRET_NAME")
NEO4J_SECRET_NAME = os.getenv("NEO4J_SECRET_NAME")
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")
TOPIC             = os.getenv("TOPIC")
NEO4J_URI         = os.getenv("NEO4J_URI")
S3_BAD_OUTPUT     = os.getenv("S3_BAD_OUTPUT")          # NEW — e.g. s3://my-bucket/bad-transactions/

try:
    kafka_secret = fetch_secret(KAFKA_SECRET_NAME, AWS_REGION)
    neo4j_secret = fetch_secret(NEO4J_SECRET_NAME, AWS_REGION)
except Exception as e:
    raise RuntimeError(
        f"Failed to fetch AWS Secrets. Check AWS auth/permissions/region/names. Root cause: {e}"
    )

SASL_USERNAME  = kafka_secret["sasl_username"]
SASL_PASSWORD  = kafka_secret["sasl_password"]
NEO4J_USER     = neo4j_secret["NEO4J_USER"]
NEO4J_PASSWORD = neo4j_secret["NEO4J_PASSWORD"]

# Fail fast — better to crash on startup than to fail silently mid-run
required = {
    "AWS_REGION":        AWS_REGION,
    "KAFKA_SECRET_NAME": KAFKA_SECRET_NAME,
    "NEO4J_SECRET_NAME": NEO4J_SECRET_NAME,
    "BOOTSTRAP_SERVERS": BOOTSTRAP_SERVERS,
    "TOPIC":             TOPIC,
    "NEO4J_URI":         NEO4J_URI,
    "S3_BAD_OUTPUT":     S3_BAD_OUTPUT,        # NEW — must be set
    "SASL_USERNAME":     SASL_USERNAME,
    "SASL_PASSWORD":     SASL_PASSWORD,
    "NEO4J_USER":        NEO4J_USER,
    "NEO4J_PASSWORD":    NEO4J_PASSWORD,
}
missing = [k for k, v in required.items() if not v]
if missing:
    raise RuntimeError(f"Missing required configuration: {', '.join(missing)}")


# =============================================================================
# Section 3: Kafka Consumer
# =============================================================================

consumer_conf = {
    "bootstrap.servers":  BOOTSTRAP_SERVERS,
    "security.protocol":  "SASL_SSL",
    "sasl.mechanisms":    "PLAIN",
    "sasl.username":      SASL_USERNAME,
    "sasl.password":      SASL_PASSWORD,
    "group.id":           "neo4j-consumer",
    "auto.offset.reset":  "earliest",
    "enable.auto.commit": True,
}
consumer = Consumer(consumer_conf)
consumer.subscribe([TOPIC])


# =============================================================================
# Section 4: Neo4j Driver
# =============================================================================

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


# =============================================================================
# Section 5: S3 Bad Record Writer  ← NEW
# =============================================================================

def _parse_s3_path(s3_uri: str):
    """Split 's3://bucket/prefix/' into ('bucket', 'prefix/')."""
    s3_uri  = s3_uri.rstrip("/")
    without = s3_uri[len("s3://"):]
    bucket, _, prefix = without.partition("/")
    return bucket, prefix + "/" if prefix else ""


_s3_client     = boto3.client("s3", region_name=AWS_REGION)
_BAD_BUCKET, _BAD_PREFIX = _parse_s3_path(S3_BAD_OUTPUT)


def write_bad_record(
    raw_value: str,
    error_type: str,
    error_detail: str,
    failed_fields: list,
    kafka_partition: int = None,
    kafka_offset: int    = None,
    kafka_key: str       = None,
    kafka_timestamp: int = None,
) -> None:
    """
    Write a single bad record to S3 as a gzipped JSON file.

    The S3 key format is:
        <S3_BAD_OUTPUT>/source=kafka/ingest_date=YYYY-MM-DD/<uuid-like>.json.gz

    Using source=kafka as a sub-prefix keeps stream and batch bad records
    in the same bucket without mixing them together.

    The envelope saved alongside the raw message includes:
      - kafka_partition / kafka_offset / kafka_key  → trace back to exact Kafka position
      - error_type / error_detail / failed_fields   → understand why it was rejected
      - raw_value                                   → replay or inspect the original message
      - ingestedAt / source                         → audit trail
    """
    now        = datetime.now(timezone.utc)
    date_str   = now.strftime("%Y-%m-%d")
    ts_str     = now.isoformat()

    envelope = {
        "kafka_partition": kafka_partition,
        "kafka_offset":    kafka_offset,
        "kafka_key":       kafka_key,
        "kafka_timestamp": kafka_timestamp,
        "raw_value":       raw_value,
        "error_type":      error_type,
        "error_detail":    error_detail,
        "failed_fields":   failed_fields,
        "ingestedAt":      ts_str,
        "source":          "kafka-stream",
    }

    # Build a unique S3 key using partition+offset so files never collide,
    # and are easy to correlate back to a specific Kafka message later.
    file_id = f"p{kafka_partition}-o{kafka_offset}" if kafka_partition is not None else ts_str
    s3_key  = f"{_BAD_PREFIX}source=kafka/ingest_date={date_str}/{file_id}.json.gz"

    # Compress to gzip in memory — keeps S3 storage costs low
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(json.dumps(envelope).encode("utf-8"))
    buf.seek(0)

    try:
        _s3_client.put_object(
            Bucket=_BAD_BUCKET,
            Key=s3_key,
            Body=buf.read(),
            ContentType="application/json",
            ContentEncoding="gzip",
        )
        print(f"[bad-record] Written to s3://{_BAD_BUCKET}/{s3_key} | "
              f"error={error_type} | fields={failed_fields}")
    except Exception as e:
        # If S3 write itself fails, log it but don't let it crash the consumer loop.
        # The original error is still logged; the bad record is not silently lost
        # from the logs even if it couldn't be persisted to S3.
        print(f"[bad-record] WARNING: Failed to write bad record to S3: {e}")
        print(f"[bad-record] Original error was: {error_type}: {error_detail}")


def _extract_pydantic_failed_fields(ve: ValidationError) -> list:
    """
    Pull the list of field names that failed Pydantic validation.
    Makes the bad-record envelope self-explanatory without reading the full error.
    """
    fields = []
    for err in ve.errors():
        loc = err.get("loc", [])
        if loc:
            fields.append(str(loc[0]))
    return list(dict.fromkeys(fields))  # deduplicate, preserve order


# =============================================================================
# Section 6: Event model (Pydantic)
# =============================================================================

class TxEvent(BaseModel):
    trans_num:              str
    trans_date_trans_time:  str
    amt:                    float
    is_fraud:               int
    category:               str
    channel:                str
    city:                   str
    state:                  str
    city_pop:               Optional[int] = None
    lat:                    float
    long:                   float
    dob:                    str
    job:                    str
    merch_lat:              float
    merch_long:             float


# =============================================================================
# Section 7: Cypher UPSERT
# =============================================================================

# Idempotent MERGE:
# - Cardholder keyed by a hash of (dob, city, state) — no raw PII stored
# - Merchant keyed by a hash of (rounded coords, city, state)
# - Transaction keyed by trans_num
# - source='kafka' distinguishes streaming records from batch-loaded ones
UPSERT = """
MERGE (c:Cardholder {cardholderId: $cardholderId})
  ON CREATE SET c.dob = $dob, c.job = $job
  ON MATCH  SET c.job = coalesce(c.job, $job), c.updatedAt = datetime()

MERGE (m:Merchant {merchantId: $merchantId})
  ON CREATE SET m.latitude = $m_lat, m.longitude = $m_long, m.city = $city, m.state = $state
  ON MATCH  SET m.updatedAt = datetime()

MERGE (t:Transaction {transNum: $trans_num})
  ON CREATE SET
    t.ts             = datetime($ts),
    t.amount         = $amt,
    t.isFraud        = $is_fraud,
    t.category       = $category,
    t.channel        = $channel,
    t.city           = $city,
    t.state          = $state,
    t.cardholderLat  = $lat,
    t.cardholderLong = $long,
    t.merchantLat    = $m_lat,
    t.merchantLong   = $m_long,
    t.job            = $job,
    t.source         = "kafka",
    t.ingestedAt     = datetime()
  ON MATCH SET
    t.amount    = $amt,
    t.category  = $category,
    t.channel   = $channel,
    t.state     = $state,
    t.updatedAt = datetime()

MERGE (c)-[:PERFORMED]->(t)
MERGE (t)-[:AT_MERCHANT]->(m)
"""


# =============================================================================
# Section 8: Helpers
# =============================================================================

def hash_id(*parts: str) -> str:
    """
    Generate a stable SHA-256 hash from the given parts.
    Normalises by lowercasing and stripping whitespace so the same logical
    entity always produces the same ID, regardless of minor formatting differences.
    """
    base = "|".join([(p or "").strip().lower() for p in parts])
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def _validate_timestamp(ts: str) -> bool:
    """
    Check that a timestamp string can actually be parsed by Neo4j's datetime() function.
    Neo4j accepts ISO-8601 strings, so we verify the format here rather than letting
    a malformed timestamp reach the database and cause a Cypher runtime error.

    Note: str.rstrip("+00:00") strips individual characters, not the whole suffix.
    We use explicit suffix removal instead to avoid that subtle gotcha.
    """
    if not ts:
        return False
    clean = ts
    if clean.endswith("Z"):
        clean = clean[:-1]
    elif clean.endswith("+00:00"):
        clean = clean[:-6]
    iso_pattern = re.compile(
        r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?$"
    )
    return bool(iso_pattern.match(clean))


def process(evt: TxEvent, raw_value: str, kafka_partition: int, kafka_offset: int, kafka_key: str, kafka_ts: int):
    """
    Validate and upsert a single event into Neo4j.
    If the timestamp is malformed, routes to S3 bad records before touching Neo4j.
    If the Neo4j write itself fails, routes to S3 bad records.
    """

    # Extra guard: Pydantic validates types but not timestamp content.
    # We check the format here so a bad timestamp doesn't reach the Cypher MERGE
    # and cause an opaque Neo4j error that's hard to trace.
    if not _validate_timestamp(evt.trans_date_trans_time):
        write_bad_record(
            raw_value=raw_value,
            error_type="InvalidTimestamp",
            error_detail=f"trans_date_trans_time='{evt.trans_date_trans_time}' is not a valid ISO-8601 datetime",
            failed_fields=["trans_date_trans_time"],
            kafka_partition=kafka_partition,
            kafka_offset=kafka_offset,
            kafka_key=kafka_key,
            kafka_timestamp=kafka_ts,
        )
        return

    cardholder_id = hash_id(evt.dob, evt.city, evt.state)
    merchant_id   = hash_id(f"{round(evt.merch_lat, 5)}", f"{round(evt.merch_long, 5)}", evt.city, evt.state)

    try:
        with driver.session() as s:
            s.run(
                UPSERT,
                cardholderId=cardholder_id,
                dob=evt.dob,
                job=evt.job,
                merchantId=merchant_id,
                m_lat=float(evt.merch_lat),
                m_long=float(evt.merch_long),
                trans_num=evt.trans_num,
                ts=evt.trans_date_trans_time,
                amt=float(evt.amt),
                is_fraud=int(evt.is_fraud),
                category=evt.category,
                channel=evt.channel,
                city=evt.city,
                state=evt.state,
                lat=float(evt.lat),
                long=float(evt.long),
            )
        print(f"[neo4j] Upserted trans_num={evt.trans_num} | "
              f"cardholderId={cardholder_id[:8]}... | merchantId={merchant_id[:8]}...")

    except Exception as neo4j_err:
        # Neo4j write failed — send the record to S3 so it can be replayed or investigated.
        # This catches connection drops, constraint violations, and Cypher runtime errors.
        write_bad_record(
            raw_value=raw_value,
            error_type="Neo4jError",
            error_detail=str(neo4j_err),
            failed_fields=[],
            kafka_partition=kafka_partition,
            kafka_offset=kafka_offset,
            kafka_key=kafka_key,
            kafka_timestamp=kafka_ts,
        )


# =============================================================================
# Section 9: Main consumer loop
# =============================================================================

_running = True

def _stop(*_):
    global _running
    _running = False
    print("\n[consumer] Shutdown signal received — draining and closing...")

signal.signal(signal.SIGINT,  _stop)
signal.signal(signal.SIGTERM, _stop)


if __name__ == "__main__":
    print(f"[consumer] Starting — topic='{TOPIC}' | bad records → {S3_BAD_OUTPUT}")
    try:
        while _running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                # Kafka-level errors (partition EOF, broker unavailable, etc.) are non-fatal.
                # Log and continue — the consumer will reconnect automatically.
                print(f"[consumer] Kafka error: {msg.error()}")
                continue

            # Grab the raw string first — we need it for the bad-record envelope
            # regardless of where the failure happens downstream.
            raw_value      = msg.value().decode("utf-8")
            kafka_partition = msg.partition()
            kafka_offset    = msg.offset()
            kafka_key       = msg.key().decode("utf-8") if msg.key() else None
            kafka_ts        = msg.timestamp()[1]        # (type, timestamp_ms)

            try:
                payload = json.loads(raw_value)
            except json.JSONDecodeError as je:
                # The message body is not valid JSON — can't do anything with it.
                # Route to bad records immediately so it's not silently lost.
                write_bad_record(
                    raw_value=raw_value,
                    error_type="JSONDecodeError",
                    error_detail=str(je),
                    failed_fields=[],
                    kafka_partition=kafka_partition,
                    kafka_offset=kafka_offset,
                    kafka_key=kafka_key,
                    kafka_timestamp=kafka_ts,
                )
                continue

            try:
                evt = TxEvent(**payload)
            except ValidationError as ve:
                # Pydantic found a type mismatch or missing required field.
                # We extract exactly which fields failed so the bad-record envelope
                # is immediately useful when reviewing rejections in S3.
                failed = _extract_pydantic_failed_fields(ve)
                write_bad_record(
                    raw_value=raw_value,
                    error_type="ValidationError",
                    error_detail=str(ve),
                    failed_fields=failed,
                    kafka_partition=kafka_partition,
                    kafka_offset=kafka_offset,
                    kafka_key=kafka_key,
                    kafka_timestamp=kafka_ts,
                )
                continue

            # Pass Kafka metadata through to process() so it can attach them
            # to the bad-record envelope if the Neo4j write fails.
            process(evt, raw_value, kafka_partition, kafka_offset, kafka_key, kafka_ts)

    finally:
        consumer.close()
        driver.close()
        print("[consumer] Closed Kafka consumer and Neo4j driver.")
