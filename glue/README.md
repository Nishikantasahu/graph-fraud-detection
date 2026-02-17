
# AWS Glue: S3 → Neo4j Transaction Ingestion Pipeline

This repository contains a Glue/Spark job (`glue_s3_to_neo4j_pass.py`) that ingests transaction-like data from **Amazon S3** (CSV, JSONL, or pretty JSON), validates and normalizes it, routes **bad records** to a separate S3 prefix, and writes **clean rows** into **Neo4j** as `(:Transaction)` nodes using the official Neo4j Spark connector.

---

## What this job does

- Recursively reads files from an S3 input prefix: **CSV**, **JSONL/NDJSON**, and **multi-line JSON**.
- Harmonizes column names across sources (e.g., `amount`/`amt`, `transaction_datetime`/`trans_date_trans_time`).
- Casts & validates types; records with casting or key-field issues are **quarantined** to a bad-records S3 prefix.
- Parses multiple timestamp formats into **epoch seconds** (robust to varied input formats).
- Upserts to Neo4j using `MERGE` on `transNum` with retry and batching.
- Optional post-write verification to log a quick count of ingested nodes.

---

## Repository layout

- `glue_s3_to_neo4j_pass.py` — the Glue/Spark job.

---

## 🔧 Required job arguments

Pass the following as **Glue Job parameters** (or CLI args when running locally):

| Argument | Description |
|---|---|
| `--S3_INPUT` | S3 prefix containing input files (recursively scanned). |
| `--S3_BAD_OUTPUT` | S3 prefix to write quarantined bad records (partitioned by date). |
| `--NEO4J_URL` | Bolt URL of Neo4j, e.g., `bolt+s://<host>:7687`. |
| `--NEO4J_DB` | Neo4j database name (usually `neo4j`). |
| `--NEO4J_SECRET_ARN` | ARN of Secrets Manager secret with Neo4j credentials. |
| `--RUN_VERIFY` | `true`/`false` to run a post-write node count check. |

**Local defaults** are present in the script only for non‑Glue runs; override for your environment.

---

##  Secrets Manager format

Create a secret whose **SecretString** is JSON:

```json
{
  "NEO4J_USER": "neo4j",
  "NEO4J_PASSWORD": "<strong_password>"
}
```

Reference its ARN via `--NEO4J_SECRET_ARN`.

---

## Supported input formats & options

- **CSV** (`*.csv`, `*.csv.gz`, `*.csv.gzip`)
  - Read **file-by-file** to avoid schema misalignment when column orders differ.
  - `header=true`, values initially read as strings for controlled casting later.
- **JSONL / NDJSON** (`*.jsonl`, `*.ndjson`, optionally gzip)
  - `primitivesAsString=true` to keep control of downstream typing.
- **Pretty / multi-line JSON** (`*.json`, optionally gzip)
  - `multiLine=true` to support objects spanning multiple lines.

Files are read **recursively** from `--S3_INPUT`.

---

##  Canonical columns & aliasing

The job standardizes headers by selecting from common aliases. Examples:

- `trans_date_trans_time` ← `trans_date`, `transaction_datetime`, `trans_date_time`
- `trans_num` ← `transNum`, `transaction_id`
- `amt` ← `amount`, `txn_amount`
- `city_pop` ← `cityPop`, `city_population`
- `is_fraud` ← `isFraud`
- `Client Name` ← `client_name`, `customer_name`
- `transaction type` ← `category`, `txn_category`

Final projection (before typing) includes:
`trans_date_trans_time`, `Client Name`, `transaction type`, `amt`, `city`, `state`, `lat`, `long`, `city_pop`, `job`, `dob`, `trans_num`, `latitude`, `longitude`, `is_fraud`, and `file`.

---

## Validation & bad-record handling

- Numeric casts: `amt`→`amount (double)`, `lat/long/latitude/longitude`→`double`, `city_pop`→`int`, `is_fraud` normalized to **0/1** from {`true/false`, `yes/no`, `y/n`, `t/f`, `1/0`}.
- Rows with **casting mismatches** are flagged with the exact problematic column names (`mismatch_cols`).
- Additional key checks route rows to bad records when any of these are missing/unparseable:
  - `trans_date_trans_time` (timestamp parse failure)
  - `trans_num`
  - `amt`
- Bad records are written to `--S3_BAD_OUTPUT` as **gzipped JSON**, partitioned by `ingest_date`, with helpful fields:
  - `ingestedAt`, `mismatch_cols`, `raw_record`, and `file` path.

---

## Timestamp parsing

A Python UDF converts many common formats to **epoch seconds** (to avoid `unix_timestamp()` hard failures under strict parsing). Supported patterns include:

- `MM/DD/YYYY HH:MM[:SS]` (24h) and `MM/DD/YYYY hh:MM[:SS] AM/PM`
- ISO-like `YYYY-MM-DD[ T ]HH:MM[:SS]`
- `DD-MM-YYYY HH:MM[:SS]`
- Slash-variants and **date-only** forms (then time is absent)

Rows that still fail parsing are quarantined as bad records.

---

## Neo4j write model

- Writes `(:Transaction {transNum})` via a single **parameterized** Cypher query.
- Uses the Neo4j Spark DataSource (v3 connector) with **basic authentication**, retries, and batching.
- Batch size: `2500` rows per transaction; writer partitions: `4` (reduces deadlocks).

**Cypher (executed per-batch internally with `UNWIND $events AS event` prepended by the connector):**

```cypher
MERGE (t:Transaction {transNum: event.transNum})
ON CREATE SET
  t.ts = datetime({epochSeconds: event.tsEpoch}),
  t.amount = event.amount,
  t.isFraud = event.isFraud,
  t.city = event.city,
  t.state = event.state,
  t.cityPop = event.cityPop,
  t.lat = event.lat,
  t.long = event.long,
  t.latitude = event.latitude,
  t.longitude = event.longitude,
  t.file = event.file,
  t.source = event.source,
  t.ingestedAt = datetime()
ON MATCH SET
  t.ts = datetime({epochSeconds: event.tsEpoch}),
  t.amount = event.amount,
  t.isFraud = event.isFraud,
  t.file = event.file,
  t.source = coalesce(t.source, event.source),
  t.updatedAt = datetime()
RETURN count(t) AS total,
       sum(CASE WHEN t.ingestedAt IS NOT NULL AND t.updatedAt IS NULL THEN 1 ELSE 0 END) AS created,
       sum(CASE WHEN t.updatedAt IS NOT NULL THEN 1 ELSE 0 END) AS updated
```

> **Heads-up:** The connector **automatically** injects `UNWIND $events AS event`. Do **not** add your own `UNWIND`, or rows will be processed twice.

---

## Post-write verification

If `--RUN_VERIFY=true`, the job runs a read query:

```cypher
MATCH (t:Transaction)
RETURN sum(CASE WHEN t.source = 'batch' THEN 1 ELSE 0 END) AS batchCount
```

The count is logged to help sanity-check ingestion without opening Neo4j Browser.

---

## Running in AWS Glue

1. Upload the script to S3 or to the Glue Job as a script file.
2. Create a **Glue Spark job** (Spark 3.x recommended).
3. Add the **Neo4j Spark Connector** JAR to the job (via **Job parameters → Dependent JARs** or an **AWS Glue job path**). Ensure version compatibility with your Spark/Scala runtime.
4. Configure **Job parameters**:
   - `--S3_INPUT=s3://<bucket>/raw/`
   - `--S3_BAD_OUTPUT=s3://<bucket>/bad-transactions/`
   - `--NEO4J_URL=bolt+s://<host>:7687`
   - `--NEO4J_DB=neo4j`
   - `--NEO4J_SECRET_ARN=arn:aws:secretsmanager:<region>:<acct>:secret:neo4j/creds`
   - `--RUN_VERIFY=true`
5. Attach an IAM Role with the permissions listed below.

### Suggested Glue job configs

- **Executor/driver memory & cores**: tune per data size. Start small (e.g., G.2X worker) and scale.
- `spark.sql.files.maxPartitionBytes = 64MB` (already set)
- `spark.sql.shuffle.partitions = 200` (adjust for cluster size)

---

## Running locally (for development)

The script detects non‑Glue runs and uses local defaults. To run with your own values:

```bash
spark-submit       --packages org.neo4j:neo4j-connector-apache-spark_2.12:<connector_version>       glue_s3_to_neo4j_pass.py       --JOB_NAME=glue_s3_to_neo4j_batch       --S3_INPUT=s3://your-bucket/raw/       --S3_BAD_OUTPUT=s3://your-bucket/bad-transactions/       --NEO4J_URL=bolt+s://<host>:7687       --NEO4J_DB=neo4j       --NEO4J_SECRET_ARN=arn:aws:secretsmanager:<region>:<acct>:secret:neo4j/creds       --RUN_VERIFY=true
```

Ensure your local environment can access AWS (for Secrets Manager) and S3.

---

## IAM permissions (minimum)

Attach to the Glue job role:

- **S3** read on `--S3_INPUT` and write on `--S3_BAD_OUTPUT` (and `_neo4j_checkpoints/`).
- **Secrets Manager**: `GetSecretValue` for `--NEO4J_SECRET_ARN`.
- **CloudWatch Logs**: to view job logs.

Example policy sketch (tighten ARNs as needed):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:ListBucket"],
      "Resource": ["arn:aws:s3:::<input-bucket>", "arn:aws:s3:::<input-bucket>/*"]
    },
    {
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:ListBucket"],
      "Resource": ["arn:aws:s3:::<bad-bucket>", "arn:aws:s3:::<bad-bucket>/*"]
    },
    {
      "Effect": "Allow",
      "Action": ["secretsmanager:GetSecretValue"],
      "Resource": "arn:aws:secretsmanager:<region>:<acct>:secret:neo4j/creds-*"
    },
    {
      "Effect": "Allow",
      "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
      "Resource": "*"
    }
  ]
}
```

---

## Quick data checks

- Verify sample payload in logs (`[neo4j-sample] ...`).
- Inspect quarantined rows under `--S3_BAD_OUTPUT/ingest_date=YYYY-MM-DD/`.
- Use `RUN_VERIFY=true` to log the count of `(:Transaction {source:'batch'})`.

---

## Troubleshooting

- **No files found**: ensure the input prefix is correct and accessible; enable the helper in code to list discovered paths.
- **Unexpected column shifts in CSV**: this job already reads CSV **per-file** to avoid schema lock-in.
- **Deadlocks in Neo4j**: the job reduces writer partitions to `4` and enables transaction retries; tune `repartition()` and `batch.size` if needed.
- **Timestamp parse failures**: confirm input formats match one of the supported patterns; otherwise extend `DATETIME_PATTERNS`.

---

## Output schema to Neo4j

Each clean row becomes (types shown in Neo4j property terms):

- `transNum` (string)
- `ts` (datetime from `tsEpoch`)
- `amount` (float)
- `isFraud` (int)
- `city` (string)
- `state` (string)
- `cityPop` (int)
- `lat`, `long`, `latitude`, `longitude` (float)
- `file` (string)
- `source` = `"batch"`
- `ingestedAt`/`updatedAt` (datetime)

---

## License

Internal/use as-is unless specified otherwise by the repository owner.

