
# Kafka → Neo4j Streaming Pipeline (with S3 bad-record routing)

This repo contains a small, production-leaning example that streams transaction events from **Kafka** into **Neo4j**. Any record that fails along the way—malformed JSON, schema validation errors, bad timestamps, or Neo4j write issues—is **captured and written to S3** as a gzipped JSON envelope **before offsets are committed**, providing an auditable, replayable trail of rejections.

## Contents
- `producer.py` – Produces synthetic transaction events to a Kafka topic, fetching SASL credentials from **AWS Secrets Manager**. citeturn1search2
- `consumer.py` – Kafka consumer that validates events, upserts them into **Neo4j**, and routes any failures to **Amazon S3** (per-record gzip). citeturn1search1
- `test_producer.py` – A targeted test harness that sends both **valid** and **intentionally bad** messages to exercise validation and the S3 bad-record path. citeturn1search3

## High-level Flow
1. **Producer** publishes JSON transactions (synthetic) to a topic (default: `transactions-stream`). Credentials come from AWS Secrets Manager; non-secrets from `.env`. citeturn1search2
2. **Consumer** reads messages, decodes JSON, validates with **Pydantic**, sanity-checks timestamp format, then **MERGE**-upserts into Neo4j. Failures are written to S3 at `s3://graph-based-transaction-fraud/fraud/bad-transactions/source=kafka/ingest_date=YYYY-MM-DD/<file>.json.gz`.
3. **Test harness** publishes a mix of records to verify: valid → Neo4j; bad → S3 with rich error envelopes. citeturn1search3

---

## Requirements
- Python 3.9+
- Kafka cluster (e.g., Confluent Cloud) with SASL/SSL enabled. citeturn1search1turn1search2
- AWS account with access to **Secrets Manager** and **S3**. citeturn1search1turn1search2
- Neo4j (Aura or self-managed), Bolt URI reachable from where the consumer runs. citeturn1search1

### Python packages
Install the usual suspects (pin versions as needed):

pip install boto3 confluent-kafka python-dotenv neo4j pydantic


## Configuration
Non-secrets come from environment variables (typically via `.env`). Secrets (Kafka SASL and Neo4j user/password) come from **AWS Secrets Manager**. The consumer **fails fast** at startup if anything essential is missing. citeturn1search1

### .env keys
Expected variables across the producer, consumer, and tester: citeturn1search1turn1search2turn1search3
dotenv
AWS_REGION=us-east-1
# Names of Secrets Manager entries holding JSON payloads
KAFKA_SECRET_NAME=<your_kafka_secret_name>
NEO4J_SECRET_NAME=<your_neo4j_secret_name>

# Kafka
BOOTSTRAP_SERVERS=<broker1:9092,broker2:9092>
TOPIC=transactions-stream

# Neo4j
NEO4J_URI=<neo4j+s://<host>:7687>

# S3 (required by consumer for bad-record routing)
S3_BAD_OUTPUT=s3://<bucket>/<prefix>
```

### Secrets Manager JSON formats
**Kafka secret** must contain: citeturn1search1turn1search2
```json
{
  "sasl_username": "<string>",
  "sasl_password": "<string>"
}
```
**Neo4j secret** must contain: citeturn1search1
```json
{
  "NEO4J_USER": "<string>",
  "NEO4J_PASSWORD": "<string>"
}
```

---

## Running the Producer
The producer emits synthetic transactions with randomised attributes. It loads `.env`, pulls SASL from Secrets Manager, and writes to the configured topic. Use `--count` and `--delay` to control volume. citeturn1search2

```bash
python producer.py --count 20 --delay 0.25
```

**Key behavior**
- Uses `SASL_SSL` with `PLAIN` mechanism. citeturn1search2
- Delivery callbacks log successes and failures; `flush(timeout=10)` ensures in-flight messages are drained on exit. citeturn1search2

---

## Running the Consumer
The consumer subscribes, validates, upserts to Neo4j, and routes any failure to S3 **with Kafka metadata** (partition, offset, key, timestamp) in the envelope. citeturn1search1

```bash
python consumer.py
```

**Validation & Safety Nets**
- **JSON decode**: non-JSON payloads → `JSONDecodeError` in S3. citeturn1search1
- **Schema**: Pydantic model enforces types & required fields → `ValidationError` with a `failed_fields` list. citeturn1search1
- **Timestamp sanity**: ISO-8601 check prevents bad timestamps from reaching Neo4j → `InvalidTimestamp`. citeturn1search1
- **Neo4j write errors** (connectivity, constraints, Cypher) → `Neo4jError`. citeturn1search1

**S3 object layout**
```
s3://<S3_BAD_OUTPUT>/
  source=kafka/
    ingest_date=YYYY-MM-DD/
      p<partition>-o<offset>.json.gz
```
Each gzipped JSON envelope includes the original `raw_value`, `error_type`, `error_detail`, `failed_fields`, Kafka metadata, and audit fields (`ingestedAt`, `source`). citeturn1search1

**Neo4j upsert (Cypher)**
- Idempotent `MERGE` for `Cardholder`, `Merchant`, and `Transaction` nodes.
- `cardholderId` is a SHA-256 hash of `(dob, city, state)`; `merchantId` is a hash of rounded `(merch_lat, merch_long, city, state)`—no raw PII stored. citeturn1search1

---

## Test Harness: Sending Valid & Bad Records
Run the test producer to emit a curated set of messages that demonstrate the rejection paths and expected outcomes. citeturn1search3

```bash
python test_producer.py
```

**What it sends**
- **3 valid** messages → should appear in Neo4j. citeturn1search3
- **8 bad** messages covering:
  - Type mismatches / missing fields (e.g., `amt` not a number, missing `trans_num`, bad `lat/long`, `is_fraud` not int). → `ValidationError` with `failed_fields`. citeturn1search3
  - Malformed timestamp → `InvalidTimestamp`. citeturn1search3
  - Raw non-JSON string → `JSONDecodeError`. citeturn1search3

It prints a post-run **checklist** with suggested S3 path inspection and a sample Neo4j query to confirm the valid ones landed. citeturn1search3

---

## Minimal IAM Permissions (reference)
- Read `secretsmanager:GetSecretValue` on the Kafka and Neo4j secret ARNs. citeturn1search1turn1search2
- Write to the S3 bucket/prefix set in `S3_BAD_OUTPUT` (PutObject). citeturn1search1

---

## Troubleshooting
- **Missing config on startup**: The consumer validates required keys and raises with the missing variable names. Check `.env` and Secret names/regions. citeturn1search1
- **Nothing in S3** while expecting rejections: Ensure `S3_BAD_OUTPUT` is set and the instance has `s3:PutObject` permissions. citeturn1search1
- **Neo4j connection errors**: Verify `NEO4J_URI`, user/password in Secrets Manager, and network reachability. citeturn1search1
- **Kafka auth**: Confirm `BOOTSTRAP_SERVERS`, `KAFKA_SECRET_NAME`, and that the secret JSON has `sasl_username`/`sasl_password`. citeturn1search1turn1search2

---

## Security Notes
- Secrets never live in the repo or `.env`; only **names** of secrets are in `.env`. Values are pulled at runtime from **AWS Secrets Manager**. citeturn1search1turn1search2
- Bad records written to S3 include the **raw Kafka message**. Ensure the bucket has appropriate access controls and, if needed, encryption & lifecycle policies. citeturn1search1

---

## License
Add your preferred license here.
