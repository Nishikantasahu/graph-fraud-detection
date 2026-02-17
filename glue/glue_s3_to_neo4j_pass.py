# =============================================================================
# AWS Glue Job: S3 → Neo4j Transaction Ingestion Pipeline
#
# What this job does:
#   - Picks up CSV, JSON, and JSONL files dropped into an S3 input folder
#   - Validates every row — bad data goes to a separate S3 bad-records folder
#   - Writes clean rows to Neo4j as Transaction nodes
#
# Job arguments you need to pass in the Glue console:
#   --S3_INPUT          S3 path to the folder containing raw files
#   --S3_BAD_OUTPUT     S3 path where rejected rows will be written
#   --NEO4J_URL         Bolt URL of your Neo4j instance
#   --NEO4J_DB          Neo4j database name (usually "neo4j")
#   --NEO4J_SECRET_ARN  ARN of the Secrets Manager secret holding Neo4j creds
#   --RUN_VERIFY        Set to "true" to log the final node count after writing
# =============================================================================

import sys
import json
import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# -----------------------------------------------------------------------------
# Section 1: Bootstrap — Glue runtime detection and job arguments
# -----------------------------------------------------------------------------



try:
    from awsglue.utils import getResolvedOptions
    from awsglue.context import GlueContext
    from awsglue.job import Job
    glue_runtime = True
except Exception:
    glue_runtime = False

REQ = [
    "JOB_NAME",
    "S3_INPUT",
    "S3_BAD_OUTPUT",
    "NEO4J_URL",
    "NEO4J_DB",
    "NEO4J_SECRET_ARN",
    "RUN_VERIFY",
]

if glue_runtime:
    args = getResolvedOptions(sys.argv, REQ)
else:
    # These defaults are only used when running locally — swap in your own values
    args = {
        "JOB_NAME":         "glue_s3_to_neo4j_batch",
        "S3_INPUT":         "s3://your-bucket/raw/",
        "S3_BAD_OUTPUT":    "s3://your-bucket/bad-transactions/",
        "NEO4J_URL":        "bolt+s://<host>:7687",
        "NEO4J_DB":         "neo4j",
        "NEO4J_SECRET_ARN": "arn:aws:secretsmanager:us-east-1:123456789012:secret:neo4j/creds",
        "RUN_VERIFY":       "true",
    }

job_name         = args["JOB_NAME"]
s3_input         = args["S3_INPUT"]
s3_bad           = args["S3_BAD_OUTPUT"]
neo4j_url        = args["NEO4J_URL"]
neo4j_db         = args["NEO4J_DB"]
neo4j_secret_arn = args["NEO4J_SECRET_ARN"]
run_verify       = str(args["RUN_VERIFY"]).lower() == "true"

# Build the Spark session.
# We set timeParserPolicy to CORRECTED so Spark uses the strict modern parser —
# this prevents it from silently accepting impossible dates like "32/01/2020".
spark = (SparkSession.builder
         .appName(job_name)
         .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
         .config("spark.sql.files.maxPartitionBytes", 64 * 1024 * 1024)
         .config("spark.sql.shuffle.partitions", "200")
         .getOrCreate())

if glue_runtime:
    glue_ctx = GlueContext(spark.sparkContext)
    job = Job(glue_ctx)
    job.init(job_name, args)

spark.sparkContext.setLogLevel("INFO")
log = spark._jvm.org.apache.log4j.LogManager.getLogger(job_name)
log.info(f"[ARGS] input={s3_input} | bad_out={s3_bad} | neo4j={neo4j_url} | db={neo4j_db} | verify={run_verify}")


# -----------------------------------------------------------------------------
# Section 2: Fetch Neo4j credentials from AWS Secrets Manager
# -----------------------------------------------------------------------------

# We never hardcode database credentials — they live in Secrets Manager.
# The secret should be a JSON string with keys: NEO4J_USER and NEO4J_PASSWORD
sm         = boto3.client("secretsmanager")
secret     = json.loads(sm.get_secret_value(SecretId=neo4j_secret_arn)["SecretString"])
neo4j_user = secret["NEO4J_USER"]
neo4j_pass = secret["NEO4J_PASSWORD"]


# -----------------------------------------------------------------------------
# Section 3: Read raw files from S3 (CSV, JSONL, JSON)
# -----------------------------------------------------------------------------

def debug_list_paths(base, limit=50):
    """Utility to log a sample of file paths found under a given S3 prefix.
    Useful when troubleshooting why certain files are not being picked up."""
    try:
        df_paths = (spark.read.format("binaryFile")
                    .option("recursiveFileLookup", "true")
                    .load(base))
        paths = [r["path"] for r in df_paths.select("path").limit(limit).collect()]
        log.info(f"[debug] Files under {base}:\n" + "\n".join(paths))
    except Exception as e:
        log.warn(f"[debug] Could not list files under {base}: {e}")

# Uncomment this if you want Spark to log every file it finds under the input path
# debug_list_paths(s3_input, limit=100)


def safe_csv_read(base):
    """Read all CSV files under the given S3 path.

    We read each file individually rather than in one bulk spark.read call.
    The reason: when files have different column orderings, Spark locks onto the
    schema of the very first file it processes and applies it positionally to
    every other file. This means a file with columns [Client Name, city, state ...]
    would get 'Client Name' shoved into the trans_date_trans_time slot, 'city' into
    amt, and so on — silently producing garbage rows that all fail validation.

    Reading file by file and then unioning means each file is parsed using its own
    header, so columns are always matched by name regardless of ordering.
    """
    try:
        file_df   = (spark.read.format("binaryFile")
                     .option("recursiveFileLookup", "true")
                     .option("pathGlobFilter", "*.{csv,csv.gz,csv.gzip}")
                     .load(base))
        csv_paths = [r["path"] for r in file_df.select("path").collect()]
    except Exception as e:
        log.warn(f"[csv] Could not list CSV files under {base}: {e}")
        return None

    if not csv_paths:
        log.warn(f"[csv] No CSV files found under {base}")
        return None

    log.info(f"[csv-read] Found {len(csv_paths)} file(s) — reading each independently")

    per_file_dfs = []
    for path in csv_paths:
        try:
            df_single = (spark.read.format("csv")
                         .option("header",      "true")
                         .option("inferSchema", "false")   # keep everything as strings for now
                         .option("multiLine",   "false")
                         .option("quote",       '"')
                         .option("escape",      '"')
                         .load(path))
            log.info(f"[csv-read] {path} → columns: {df_single.columns}")
            per_file_dfs.append(df_single)
        except Exception as e:
            log.warn(f"[csv-read] Skipping {path}: {e}")

    if not per_file_dfs:
        return None

    # Merge all per-file dataframes — missing columns in any file become null
    result = per_file_dfs[0]
    for d in per_file_dfs[1:]:
        result = result.unionByName(d, allowMissingColumns=True)
    return result


def safe_jsonl_read(base):
    """Read newline-delimited JSON files (.jsonl / .ndjson).
    All values are read as strings so we control casting ourselves downstream."""
    try:
        return (spark.read.format("json")
                .option("primitivesAsString", "true")
                .option("multiLine",          "false")
                .option("recursiveFileLookup","true")
                .option("pathGlobFilter",     "*.{jsonl,ndjson,jsonl.gz,ndjson.gz}")
                .load(base))
    except Exception as e:
        log.warn(f"[jsonl] Read failed under {base}: {e}")
        return None


def safe_json_pretty_read(base):
    """Read pretty-printed / multi-line JSON files (.json).
    multiLine=true tells Spark a single JSON object may span multiple lines."""
    try:
        return (spark.read.format("json")
                .option("primitivesAsString", "true")
                .option("multiLine",          "true")
                .option("recursiveFileLookup","true")
                .option("pathGlobFilter",     "*.{json,json.gz}")
                .load(base))
    except Exception as e:
        log.warn(f"[json] Read failed under {base}: {e}")
        return None


# Attempt to read all three formats and combine whatever was found
dfs = []
for reader in [safe_csv_read, safe_jsonl_read, safe_json_pretty_read]:
    result = reader(s3_input)
    if result is not None:
        dfs.append(result)

if not dfs:
    raise ValueError(f"No readable files (CSV / JSON / JSONL) found under: {s3_input}")

# Merge all format dataframes into one — columns that don't exist in a given
# format will just appear as null, which is fine at this stage
df_raw = dfs[0]
for d in dfs[1:]:
    df_raw = df_raw.unionByName(d, allowMissingColumns=True)

# Attach the source file path so we can trace any bad record back to its file
df_raw = df_raw.withColumn("file", F.input_file_name())


# -----------------------------------------------------------------------------
# Section 4: Standardise column names across different file formats
# -----------------------------------------------------------------------------

# Some source files use different column names for the same field.
# We strip any accidental whitespace from headers first ...
for c in df_raw.columns:
    if c.strip() != c:
        df_raw = df_raw.withColumnRenamed(c, c.strip())

# ... then define which alternative names each canonical field can appear under.
# The first name in each list that actually exists in the data wins.
aliases = {
    "trans_date_trans_time": ["trans_date_trans_time", "trans_date", "transaction_datetime", "trans_date_time"],
    "trans_num":             ["trans_num", "transNum", "transaction_id"],
    "amt":                   ["amt", "amount", "txn_amount"],
    "city_pop":              ["city_pop", "cityPop", "city_population"],
    "is_fraud":              ["is_fraud", "isFraud"],
    "Client Name":           ["Client Name", "client_name", "customer_name"],
    "transaction type":      ["transaction type", "category", "txn_category"],
    "job":                   ["job", "occupation"],
    "dob":                   ["dob", "date_of_birth"],
    # Fields below always use the same name across all formats
    "lat":       ["lat"],
    "long":      ["long"],
    "latitude":  ["latitude"],
    "longitude": ["longitude"],
    "city":      ["city"],
    "state":     ["state"],
}

def pick(df, candidates):
    """Return the first column name from candidates that exists in df, or None."""
    for name in candidates:
        if name in df.columns:
            return name
    return None

# Build a mapping from canonical name → actual column name found in the data
sel_map = {k: pick(df_raw, lst) for k, lst in aliases.items() if pick(df_raw, lst)}

# Project to just the columns we care about, renaming to canonical names
required = [
    "trans_date_trans_time", "Client Name", "transaction type", "amt",
    "city", "state", "lat", "long", "city_pop", "job", "dob",
    "trans_num", "latitude", "longitude", "is_fraud", "file"
]
df = df_raw.select([
    F.col(sel_map.get(c, c)).alias(c) if c != "file" else F.col("file")
    for c in required
    if c in sel_map or c == "file"
])


# -----------------------------------------------------------------------------
# Section 5: Cast numeric fields and detect bad values
# -----------------------------------------------------------------------------

def as_double(c): return F.col(c).cast("double")
def as_int(c):    return F.col(c).cast("int")

# is_fraud comes in many shapes — "yes", "Y", "true", "1", "false", "no", etc.
# We normalise all of them down to 1 or 0. Anything unrecognised becomes null (= bad record).
is_fraud_norm = (
    F.when(F.lower(F.col("is_fraud")).isin("true", "yes", "y", "t", "1"), F.lit(1))
     .when(F.lower(F.col("is_fraud")).isin("false", "no", "n", "f", "0"), F.lit(0))
     .otherwise(F.col("is_fraud").cast("int"))
).alias("isFraud")

df_typed = (df
    .withColumn("amount",      as_double("amt"))
    .withColumn("lat_d",       as_double("lat"))
    .withColumn("long_d",      as_double("long"))
    .withColumn("latitude_d",  as_double("latitude"))
    .withColumn("longitude_d", as_double("longitude"))
    .withColumn("cityPop",     as_int("city_pop"))
    .withColumn("isFraud",     is_fraud_norm)
)

def mismatch_for(original_col, casted_col):
    """Returns True when a field had a non-empty value that failed casting.
    Empty strings are skipped — we only flag genuine bad values, not missing ones."""
    return (
        F.col(original_col).isNotNull() &
        (F.length(F.trim(F.col(original_col))) > 0) &
        F.col(casted_col).isNull()
    )

# For each field pair, we'll record the field name when a mismatch is detected.
# This makes it easy to see exactly which fields caused a row to be rejected.
mismatch_specs = [
    ("amt",       "amount"),
    ("lat",       "lat_d"),
    ("long",      "long_d"),
    ("latitude",  "latitude_d"),
    ("longitude", "longitude_d"),
    ("city_pop",  "cityPop"),
    ("is_fraud",  "isFraud"),
]

mismatch_flags = [F.when(mismatch_for(o, c), F.lit(o)) for o, c in mismatch_specs]

df_mm = (df_typed
    .withColumn("mismatch_cols_arr", F.array(*mismatch_flags))
    .withColumn("mismatch_cols",     F.expr("filter(mismatch_cols_arr, x -> x is not null)"))
    .drop("mismatch_cols_arr")
)

# Split into bad rows (at least one field failed) and good rows (everything clean)
bad_df  = df_mm.where(F.size(F.col("mismatch_cols")) > 0)
good_df = df_mm.where(F.size(F.col("mismatch_cols")) == 0)

# Enrich bad rows with a timestamp and the full original record before saving
bad_df = (bad_df
    .withColumn("ingestedAt",  F.current_timestamp())
    .withColumn("raw_record",  F.to_json(F.struct([F.col(c) for c in df.columns])))
)

# Write to S3, partitioned by date so they're easy to find and query later
(bad_df
 .withColumn("ingest_date", F.to_date(F.col("ingestedAt")))
 .write.mode("append")
 .option("compression", "gzip")
 .partitionBy("ingest_date")
 .json(s3_bad))

log.info(f"[bad-records] Rows with field mismatches written to: {s3_bad}")


# -----------------------------------------------------------------------------
# Section 6: Parse timestamps into epoch seconds
# -----------------------------------------------------------------------------

# Why a Python UDF instead of Spark's built-in unix_timestamp()?
# With timeParserPolicy=CORRECTED, unix_timestamp() throws a hard exception
# when a value doesn't match the expected pattern exactly — it doesn't just return null,
# it crashes the entire Spark task. A Python UDF with try/except handles each value
# independently and returns None on failure, which Spark turns into a clean null.
# That null then routes the row to bad records without taking anything else down with it.

from pyspark.sql.types import LongType
from datetime import datetime

# List all timestamp formats we want to accept.
# Ordered most-specific to least-specific — first match wins.
DATETIME_PATTERNS = [
    "%m/%d/%Y %H:%M:%S",    # 01/15/2020 14:32:00
    "%m/%d/%Y %H:%M",       # 01/15/2020 14:32  (also handles single-digit hours like 9:05)
    "%m/%d/%Y %I:%M:%S %p", # 01/15/2020 02:30:00 PM
    "%m/%d/%Y %I:%M %p",    # 01/15/2020 02:30 PM
    "%Y-%m-%d %H:%M:%S",    # 2020-01-15 14:32:00
    "%Y-%m-%dT%H:%M:%S",    # 2020-01-15T14:32:00
    "%Y-%m-%d %H:%M",       # 2020-01-15 14:32
    "%Y-%m-%dT%H:%M",       # 2020-01-15T14:32
    "%d-%m-%Y %H:%M:%S",    # 15-01-2020 14:32:00
    "%d-%m-%Y %H:%M",       # 15-01-2020 14:32
    "%Y/%m/%d %H:%M:%S",    # 2020/01/15 14:32:00
    "%Y/%m/%d %H:%M",       # 2020/01/15 14:32
    "%m/%d/%Y",             # 01/15/2020  (date only)
    "%Y-%m-%d",             # 2020-01-15  (date only)
    "%Y/%m/%d",             # 2020/01/15  (date only)
    "%d-%m-%Y",             # 15-01-2020  (date only)
]

def safe_parse_epoch(ts_str):
    """Try to parse a timestamp string into Unix epoch seconds.
    Returns None if the string is empty or doesn't match any known format — the
    calling code will route that row to bad records."""
    if ts_str is None or not ts_str.strip():
        return None
    for fmt in DATETIME_PATTERNS:
        try:
            return int(datetime.strptime(ts_str.strip(), fmt).timestamp())
        except (ValueError, OverflowError):
            continue
    return None

safe_epoch_udf = F.udf(safe_parse_epoch, LongType())

good_df = (good_df
    .withColumn("raw_ts",   F.col("trans_date_trans_time").cast("string"))
    .withColumn("tsEpoch",  safe_epoch_udf(F.col("raw_ts")))
    .withColumn("tsStr",    F.when(
                                F.col("tsEpoch").isNotNull(),
                                F.from_unixtime(F.col("tsEpoch"), "yyyy-MM-dd'T'HH:mm:ss")
                            ))
)

# Catch any rows where timestamp couldn't be parsed, or where a key field is missing.
# We record which field was the problem so it shows up clearly in the bad-records file.
null_field_bad = (good_df
    .where(
        F.col("tsEpoch").isNull() |
        F.col("trans_num").isNull() |
        F.col("amount").isNull()
    )
    .withColumn("ingestedAt", F.current_timestamp())
    .withColumn("mismatch_cols",
        F.when(F.col("tsEpoch").isNull(),   F.array(F.lit("trans_date_trans_time")))
         .when(F.col("trans_num").isNull(), F.array(F.lit("trans_num")))
         .when(F.col("amount").isNull(),    F.array(F.lit("amt")))
         .otherwise(F.array())
    )
    .withColumn("raw_record", F.to_json(F.struct([
        F.col(c) for c in df.columns if c in good_df.columns
    ])))
)

if null_field_bad.count() > 0:
    (null_field_bad
     .withColumn("ingest_date", F.to_date(F.col("ingestedAt")))
     .write.mode("append")
     .option("compression", "gzip")
     .partitionBy("ingest_date")
     .json(s3_bad))
    log.info(f"[bad-records] Rows with unparseable timestamps or missing keys written to: {s3_bad}")
else:
    log.info("[bad-records] No timestamp or key issues found — all remaining rows are clean.")

# Keep only the rows that passed every check and are ready for Neo4j
filtered = good_df.where(
    F.col("trans_num").isNotNull() &
    F.col("tsEpoch").isNotNull() &
    F.col("amount").isNotNull()
)


# -----------------------------------------------------------------------------
# Section 7: Build the final Neo4j payload
# -----------------------------------------------------------------------------

# Select only the fields Neo4j needs and give them their final names.
# We cast everything explicitly here — Neo4j is strict about types.
to_write = filtered.select(
    F.col("trans_num").alias("transNum"),
    F.col("tsEpoch").cast("long"),
    F.col("amount").cast("double"),
    F.col("isFraud").cast("int"),
    F.col("city").cast("string"),
    F.col("state").cast("string"),
    F.col("cityPop").cast("int"),
    F.col("lat_d").alias("lat").cast("double"),
    F.col("long_d").alias("long").cast("double"),
    F.col("latitude_d").alias("latitude").cast("double"),
    F.col("longitude_d").alias("longitude").cast("double"),
    F.col("file").cast("string"),
    F.lit("batch").alias("source"),
    F.current_timestamp().alias("ingestedAt"),
)

# Log a sample row so we can sanity-check the payload shape in CloudWatch
try:
    sample = to_write.limit(1).toJSON().collect()
    if sample:
        log.info(f"[neo4j-sample] {sample[0]}")
except Exception as e:
    log.warn(f"[neo4j-sample] Could not collect sample row: {e}")


# -----------------------------------------------------------------------------
# Section 8: Write to Neo4j
# -----------------------------------------------------------------------------

# Connection settings — credentials come from Secrets Manager (Section 2)
neo4j_opts = {
    "url":                           neo4j_url,
    "database":                      neo4j_db,
    "authentication.type":           "basic",
    "authentication.basic.username": neo4j_user,
    "authentication.basic.password": neo4j_pass,
    "checkpoint.location":           s3_input.rstrip("/") + "/_neo4j_checkpoints/",
}

# Important: the Neo4j Spark connector automatically prepends
#   WITH $scriptResult AS scriptResult
#   UNWIND $events AS event
# before running your query. So start your query directly with MERGE/MATCH
# and use the variable name 'event' to reference each row's fields.
# Adding your own UNWIND causes every row to be processed twice —
# the second pass finds the node already exists, hits ON MATCH instead of
# ON CREATE, and your node count never grows even though data was "written".
query = """
MERGE (t:Transaction {transNum: event.transNum})
  ON CREATE SET
    t.ts         = datetime({epochSeconds: event.tsEpoch}),
    t.amount     = event.amount,
    t.isFraud    = event.isFraud,
    t.city       = event.city,
    t.state      = event.state,
    t.cityPop    = event.cityPop,
    t.lat        = event.lat,
    t.long       = event.long,
    t.latitude   = event.latitude,
    t.longitude  = event.longitude,
    t.file       = event.file,
    t.source     = event.source,
    t.ingestedAt = datetime()
  ON MATCH SET
    t.ts         = datetime({epochSeconds: event.tsEpoch}),
    t.amount     = event.amount,
    t.isFraud    = event.isFraud,
    t.file       = event.file,
    t.source     = coalesce(t.source, event.source),
    t.updatedAt  = datetime()
RETURN count(t)                                                                          AS total,
       sum(CASE WHEN t.ingestedAt IS NOT NULL AND t.updatedAt IS NULL THEN 1 ELSE 0 END) AS created,
       sum(CASE WHEN t.updatedAt  IS NOT NULL                         THEN 1 ELSE 0 END) AS updated
"""

# We use 4 partitions rather than the default 8.
# Fewer parallel writers = fewer concurrent MERGE transactions hitting Neo4j at once,
# which reduces the chance of deadlock errors (ForsetiClient lock cycle exceptions).
to_write_repart = to_write.repartition(4)

(to_write_repart.write
    .format("org.neo4j.spark.DataSource")
    .mode("Append")
    .options(**neo4j_opts)
    .option("query",                     query)
    .option("batch.size",                "2500")  # rows per transaction
    .option("transaction.retries",       "10")    # retry on transient errors like deadlocks
    .option("transaction.retry.timeout", "30000") # wait 30s before each retry
    .save()
)

log.info("[neo4j] Write completed.")


# -----------------------------------------------------------------------------
# Section 9: Optional post-write verification
# -----------------------------------------------------------------------------

# If RUN_VERIFY is true, we query Neo4j for the total node count and log it.
# This is a quick sanity check — saves you from having to open the Neo4j browser.
if run_verify:
    verify_query = """
    MATCH (t:Transaction)
    RETURN sum(CASE WHEN t.source = 'batch' THEN 1 ELSE 0 END) AS batchCount
    """
    df_verify = (spark.read
        .format("org.neo4j.spark.DataSource")
        .options(**neo4j_opts)
        .option("query", verify_query)
        .load())
    rows  = df_verify.collect()
    count = rows[0]["batchCount"] if rows else "N/A"
    log.info(f"[verify] Total Transaction nodes with source='batch' in Neo4j: {count}")

if glue_runtime:
    job.commit()
