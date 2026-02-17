"""
test_producer.py — Bad record test harness for the Kafka → Neo4j streaming pipeline.


"""

import json
import os
import time
import base64

import boto3
from confluent_kafka import Producer
from dotenv import load_dotenv

# =============================================================================
# Config — same setup as producer.py
# =============================================================================

load_dotenv()

AWS_REGION        = os.getenv("AWS_REGION", "us-east-1")
KAFKA_SECRET_NAME = os.getenv("KAFKA_SECRET_NAME")
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")
TOPIC             = os.getenv("TOPIC", "transactions-stream")


def fetch_secret(secret_name: str, region: str) -> dict:
    client = boto3.client("secretsmanager", region_name=region)
    resp   = client.get_secret_value(SecretId=secret_name)
    if "SecretString" in resp:
        return json.loads(resp["SecretString"])
    return json.loads(base64.b64decode(resp["SecretBinary"]))


secret        = fetch_secret(KAFKA_SECRET_NAME, AWS_REGION)
SASL_USERNAME = secret["sasl_username"]
SASL_PASSWORD = secret["sasl_password"]

conf = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms":   "PLAIN",
    "sasl.username":     SASL_USERNAME,
    "sasl.password":     SASL_PASSWORD,
    "client.id":         "test-bad-record-producer",
}
producer = Producer(conf)


# =============================================================================
# Delivery report callback
# =============================================================================

def dr_cb(err, msg):
    if err:
        print(f"  ❌ Delivery FAILED: {err}")
    else:
        print(f"  ✅ Delivered → partition={msg.partition()} offset={msg.offset()}")


# =============================================================================
# Base valid record — all tests mutate from this
# =============================================================================

BASE = {
    "trans_num":              "T-BASE",
    "trans_date_trans_time":  "2026-02-17T10:00:00.000001Z",  # microseconds match real producer format
    "amt":                    99.99,
    "is_fraud":               0,
    "category":               "GROCERY",
    "channel":                "POS",
    "city":                   "Denver",
    "state":                  "CO",
    "city_pop":               616596,
    "lat":                    39.7392,
    "long":                   -104.9903,
    "dob":                    "1985-06-15",
    "job":                    "Engineer",
    "merch_lat":              39.8,
    "merch_long":             -104.9,
}


# =============================================================================
# Test messages
# Each entry is (label, key, payload_or_raw_string, expected_consumer_outcome)
# payload_or_raw_string:
#   - dict  → will be json.dumps()'d before producing (normal path)
#   - str   → produced as-is (used for the not-JSON test case)
# =============================================================================

TEST_MESSAGES = [

    # ── VALID RECORDS ─────────────────────────────────────────────────────────
    # These should pass all validation and reach Neo4j.

    (
        "V1  Valid — standard record",
        "T-VALID-001",
        {**BASE, "trans_num": "T-VALID-001", "amt": 52.30},
        "→ Neo4j ✅",
    ),
    (
        "V2  Valid — single-digit hour timestamp",
        "T-VALID-002",
        {**BASE, "trans_num": "T-VALID-002", "amt": 7.99,
         "trans_date_trans_time": "2026-02-17T09:05:00.000001Z"},   # 09 is valid two-digit hour
        "→ Neo4j ✅",
    ),
    (
        "V3  Valid — SHOPPING category, is_fraud=0",
        "T-VALID-003",
        {**BASE, "trans_num": "T-VALID-003", "amt": 210.00,
         "category": "SHOPPING", "channel": "NET"},
        "→ Neo4j ✅",
    ),

    # ── BAD: ValidationError ──────────────────────────────────────────────────
    # Pydantic rejects these because field types don't match the model.

    (
        "#1  Bad amount — amt='not_a_number'",
        "T-BAD-AMT-001",
        {**BASE, "trans_num": "T-BAD-AMT-001",
         "amt": "not_a_number"},          # Pydantic expects float
        "ValidationError: ['amt']",
    ),
    (
        "#2  Missing trans_num — key absent from payload",
        "T-BAD-NOKEY-001",
        {k: v for k, v in {**BASE, "trans_num": "T-BAD-NOKEY-001"}.items()
         if k != "trans_num"},            # remove trans_num entirely
        "ValidationError: ['trans_num']",
    ),
    (
        "#3  Bad lat/long — both are non-numeric strings",
        "T-BAD-GEO-001",
        {**BASE, "trans_num": "T-BAD-GEO-001",
         "lat": "not_a_lat",
         "long": "bad_long"},             # Pydantic expects float for both
        "ValidationError: ['lat', 'long']",
    ),
    (
        "#4  Bad is_fraud — string that isn't castable to int",
        "T-BAD-FRAUD-001",
        {**BASE, "trans_num": "T-BAD-FRAUD-001",
         "is_fraud": "maybe"},            # Pydantic expects int, 'maybe' can't cast
        "ValidationError: ['is_fraud']",
    ),
    (
        "#7  Multiple bad fields — amt, lat, and is_fraud all wrong",
        "T-BAD-MULTI-001",
        {**BASE, "trans_num": "T-BAD-MULTI-001",
         "amt":      "NaN",
         "lat":      "abc",
         "is_fraud": "yes-maybe"},        # all three fail type cast
        "ValidationError: ['amt', 'lat', 'is_fraud']",
    ),
    (
        "#8  Null amt — amt is JSON null (None in Python)",
        "T-BAD-NULL-001",
        {**BASE, "trans_num": "T-BAD-NULL-001",
         "amt": None},                    # required float field is null
        "ValidationError: ['amt']",
    ),

    # ── BAD: InvalidTimestamp ─────────────────────────────────────────────────
    # Pydantic accepts 'str' so this passes schema validation,
    # but _validate_timestamp() catches it before Neo4j is touched.

    (
        "#5  Bad timestamp — not a recognisable datetime format",
        "T-BAD-TS-001",
        {**BASE, "trans_num": "T-BAD-TS-001",
         "trans_date_trans_time": "not-a-date"},   # passes Pydantic str check, fails regex
        "InvalidTimestamp: ['trans_date_trans_time']",
    ),

    # ── BAD: JSONDecodeError ──────────────────────────────────────────────────
    # The message body is a raw string, not JSON.
    # json.loads() throws immediately in the consumer loop.
    # We produce this as a raw string (no json.dumps wrapping).

    (
        "#6  Not JSON — raw string body",
        "T-BAD-JSON-001",
        "this is not json at all",         # str, not dict — produced as-is
        "JSONDecodeError",
    ),
]


# =============================================================================
# Produce all test messages
# =============================================================================

def produce_all():
    print(f"\nProducing {len(TEST_MESSAGES)} test message(s) to topic '{TOPIC}'")
    print(f"  3 valid  → should reach Neo4j")
    print(f"  8 bad    → should land in S3 bad-transactions/source=kafka/")
    print()

    for label, key, payload, expected in TEST_MESSAGES:
        # Encode payload — dicts become JSON strings, raw strings go as-is
        if isinstance(payload, dict):
            value = json.dumps(payload)
        else:
            value = payload             # already a raw string for the JSON test

        print(f"  Sending: {label}")
        print(f"    key={key}  |  expected → {expected}")

        producer.produce(
            TOPIC,
            key=key,
            value=value.encode("utf-8"),
            callback=dr_cb,
        )
        producer.poll(0)
        time.sleep(0.3)     # small gap so delivery logs stay readable

    # Block until all pending messages are confirmed delivered or timed out
    print("\nFlushing ...")
    remaining = producer.flush(timeout=15)
    if remaining:
        print(f"⚠️  {remaining} message(s) were not confirmed within the flush timeout.")
    else:
        print("All messages delivered.")


# =============================================================================
# What to check after running
# =============================================================================

CHECKLIST = """
After running this script, verify the following:

  IN S3 (8 files expected):
    s3://graph-based-transaction-fraud/fraud/bad-transactions/source=kafka/ingest_date=<today>/
    ┌──────────────────────┬──────────────────────────────────────────────┐
    │ Key sent             │ Expected error_type in S3 envelope           │
    ├──────────────────────┼──────────────────────────────────────────────┤
    │ T-BAD-AMT-001        │ ValidationError  — failed_fields: ["amt"]    │
    │ T-BAD-NOKEY-001      │ ValidationError  — failed_fields: ["trans_num"] │
    │ T-BAD-GEO-001        │ ValidationError  — failed_fields: ["lat","long"] │
    │ T-BAD-FRAUD-001      │ ValidationError  — failed_fields: ["is_fraud"] │
    │ T-BAD-MULTI-001      │ ValidationError  — failed_fields: ["amt","lat","is_fraud"] │
    │ T-BAD-NULL-001       │ ValidationError  — failed_fields: ["amt"]    │
    │ T-BAD-TS-001         │ InvalidTimestamp — failed_fields: ["trans_date_trans_time"] │
    │ T-BAD-JSON-001       │ JSONDecodeError  — failed_fields: []         │
    └──────────────────────┴──────────────────────────────────────────────┘

  IN NEO4J (3 nodes expected):
    MATCH (t:Transaction)
    WHERE t.transNum IN ['T-VALID-001', 'T-VALID-002', 'T-VALID-003']
    RETURN t.transNum, t.source, t.ingestedAt
    → should return 3 rows with source='kafka'

  QUICK S3 INSPECTION (decompress one file):
    aws s3 cp s3://graph-based-transaction-fraud/fraud/bad-transactions/source=kafka/ingest_date=<today>/T-BAD-AMT-001.json.gz - | gunzip
"""

if __name__ == "__main__":
    produce_all()
    print(CHECKLIST)
