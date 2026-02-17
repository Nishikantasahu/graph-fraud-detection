
# Graph-Based Transaction Fraud Detection Pipeline

This repository documents an end‑to‑end data ingestion and quality‑control framework designed for graph‑based fraud analytics using **Neo4j**.
It combines both **batch** and **streaming** ingestion paths, ensures strict data validation, isolates bad data for inspection, and produces an automated daily reconciliation summary.

The overall goal of this system is simple:
> **Ingest clean, well‑structured transactional data into Neo4j from batch and real time—while preserving auditability, governance, and trust in the dataset.**

---

## 📌 High‑Level Architecture

> **[Insert Architecture Diagram Here]**  
> *(Kafka → Consumer → Neo4j, S3 Raw → Glue → Neo4j, Bad‑Data S3, Reconciliation Jobs, etc.)*

The system operates through two pipelines:

### 1. Streaming Pipeline
**Confluent Cloud Kafka → Python Consumer → Neo4j**

### 2. Batch Pipeline
**AWS S3 → AWS Glue ETL → Neo4j**

Both pipelines share:
- A common Neo4j graph model  
- Centralized schema validation  
- Unified handling of bad data  
- Source attribution (`kafka` or `batch`) for downstream analysis  
- Alignment with the daily reconciliation workflow

---

# 1. Streaming Pipeline (Kafka → Neo4j)

The streaming pipeline is built for continuous ingestion of real‑time transactions from **Confluent Cloud**.

## 1.1 Kafka Producer
_File: `producer.py`_

The producer creates synthetic financial transactions and publishes them to Kafka using:
- SASL/SSL authentication (credentials securely pulled from AWS Secrets Manager)
- Configurable event volume and publishing frequency
- Clean JSON events with a consistent schema

> **[Insert Screenshot – Producer Terminal / .env Variables]**

## 1.2 Kafka Consumer
_File: `consumer.py`_

The consumer drives most of the intelligent behavior in the streaming path. It:

### ✔ Validates incoming messages
- JSON decoding  
- Schema validation using **Pydantic**  
- Timestamp verification (ISO‑8601)

### ✔ Constructs graph‑safe IDs
- `cardholderId` → hash of DOB + city + state  
- `merchantId` → hash of rounded merchant coordinates + city + state

### ✔ Performs idempotent MERGE operations in Neo4j
Ensures:
- No duplicate cardholders  
- No duplicate merchants  
- No duplicate transactions  

All updates include `ingestedAt` + optional `updatedAt` timestamps.

### ✔ Routes bad data to S3
Any message that fails validation or Neo4j insertion is written to:

```
s3://graph-based-transaction-fraud/fraud/bad-transactions/
    source=kafka/
        ingest_date=YYYY-MM-DD/
            p<partition>-o<offset>.json.gz
```

Each envelope includes:
- Raw Kafka message  
- Error type + explanation  
- Failed fields  
- Kafka metadata (partition, offset, key, timestamp)

> **[Insert Screenshot – S3 Bad Records (Kafka)]**  
> **[Insert Screenshot – Neo4j Browser (Kafka Transaction Nodes)]**

---

# 2. Batch Pipeline (S3 → AWS Glue → Neo4j)

_File: `glue_s3_to_neo4j_pass.py`_

The batch pipeline handles historical uploads, bulk loads, and periodic refreshes. It is intentionally resilient to messy, inconsistent file formats.

## 2.1 Supported Inputs
- CSV  
- JSONL / NDJSON  
- Pretty JSON (multiline)  
- Files with differing column order  
- Files with inconsistent field naming conventions

## 2.2 Standardization & Validation
The Glue job performs:

### ✔ Header Normalization
Trims whitespace & maps various alias names to canonical fields.

### ✔ Per‑File CSV Reading
Avoids schema corruption from column reordering.

### ✔ Safe Type Casting
Detects mismatches for:
- Numeric fields  
- Latitude/longitude  
- City population  
- Fraud flag variations (“yes/No/TRUE/0”)

### ✔ Safe Timestamp Parsing
Multiple timestamp patterns supported using a custom UDF—so bad values don’t crash the job.

### ✔ Bad‑Record Routing
Bad rows go to:

```
s3://graph-based-transaction-fraud/fraud/bad-transactions/
    source=batch/
        ingest_date=YYYY-MM-DD/
```

Each rejected row includes:
- List of mismatched fields  
- Full raw record  
- Timestamp of ingestion

## 2.3 Neo4j Write
The Glue job writes validated rows to Neo4j through the Spark connector using:

- Controlled write partitions  
- Batch writes (2500 rows)  
- Automatic retry logic for transient lock conflicts  
- Explicit field casting  
- `source = "batch"` attribution

> **[Insert Screenshot – Glue Job Config]**  
> **[Insert Screenshot – S3 Bad Records (Batch)]**  
> **[Insert Screenshot – Neo4j Browser (Batch Transaction Nodes)]**

---

# 3. Unified Bad‑Data Handling

Both pipelines write malformed or rejected records to the same S3 bucket:

```
s3://graph-based-transaction-fraud/fraud/bad-transactions/
```

Partitioned by:
- **source = kafka | batch**  
- **ingest_date = YYYY-MM-DD**

This makes cross‑source quality comparisons trivial and supports downstream reconciliation.

> **[Insert Screenshot – S3 Folder Hierarchy]**

---

# 4. Daily Automated Reconciliation

A scheduled **AWS EventBridge → Lambda** workflow generates daily reconciliation results stored in:

```
s3://neo4j-reconciliation-daily/reconciliation/
    dt=YYYY-MM-DD/
```

The report includes:
- Record counts by source  
- Duplicate counts  
- Field mismatch stats  
- Streaming ingestion lag distribution  
- Summary pass/fail metrics

These results provide an auditable health snapshot for each day's data.

> **[Insert Screenshot – Reconciliation Report Example]**  
> **[Insert Screenshot – S3 Reconciliation Partitions]**

---

# 5. Neo4j Graph Model Overview

> **[Insert Diagram – Cardholder → Transaction → Merchant]**

## Nodes
- **Cardholder**  
  - Key: SHA‑256 hash of DOB + city + state  
- **Merchant**  
  - Key: SHA‑256 hash of merchant location + city + state  
- **Transaction**  
  - Key: `transNum`  
  - Attributes vary slightly across batch vs streaming ingestion

## Relationships
- `(Cardholder)-[:PERFORMED]->(Transaction)`  
- `(Transaction)-[:AT_MERCHANT]->(Merchant)`

## Source Attribution
- `source = "kafka"`  
- `source = "batch"`

---

# 6. How to Run

## Streaming
1. Configure `.env` with Kafka parameters.
2. Ensure Kafka/Neo4j secrets exist in AWS Secrets Manager.
3. Start the **consumer** first.
4. Run the **producer** to generate events.
5. Validate data flow in Neo4j & S3.

## Batch
1. Upload raw files to the input S3 folder.
2. Run the Glue job with proper arguments.
3. Check S3 for bad records.
4. View `Transaction` nodes in Neo4j.

---

# 7. Screenshot Drop‑Zones

Use the placeholders below when preparing documentation:

```
[ Insert Architecture Diagram Here ]
[ Insert Kafka Topic Screenshot Here ]
[ Insert Producer/Consumer Terminal Logs Here ]
[ Insert S3 Bad Records Screenshots Here ]
[ Insert Neo4j Graph Browser Screenshots Here ]
[ Insert Glue Job Config Screenshots Here ]
[ Insert Reconciliation Reports Screenshots Here ]
```

---

# 8. Cypher Query Workspace

Paste your analytical queries here (fraud patterns, velocity checks, merchant risk scoring, ring detection, etc.)

```cypher
// Example: Count by source
MATCH (t:Transaction)
RETURN t.source AS source, count(*) AS cnt
ORDER BY cnt DESC;
```

```cypher
// Example: Merchant fanout
MATCH (c:Cardholder)-[:PERFORMED]->(t)-[:AT_MERCHANT]->(m)
RETURN c.cardholderId AS cardholder, count(DISTINCT m) AS merchantCount
ORDER BY merchantCount DESC
LIMIT 20;
```

> **[Paste your Cypher queries below]**

---

# 9. Summary

This system is designed to be robust, transparent, and production‑ready:

- Handles both batch and streaming ingestion  
- Validates data thoroughly  
- Prevents schema violations  
- Ensures idempotent graph updates  
- Surfaces all quality issues with rich metadata  
- Produces daily governance reports

The result is a clean, trustworthy Neo4j dataset powering advanced fraud analytics.

---

## Want help finishing touches?
- I can embed your screenshots, add a Table of Contents, and generate a PDF once you share the images.
