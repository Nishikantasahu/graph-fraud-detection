"""
Kafka Transaction Producer

- Loads non-secret config from .env
- Fetches Kafka SASL credentials from AWS Secrets Manager
- Produces synthetic transaction events to a Kafka topic

"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import time
import uuid
from base64 import b64decode
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Tuple

import boto3
from confluent_kafka import Producer
from dotenv import load_dotenv

# --------------------------- Logging setup --------------------------- #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("tx-producer")

# --------------------------- Configuration --------------------------- #
@dataclass(frozen=True)
class AppConfig:
    """Holds runtime configuration values."""

    aws_region: str
    kafka_secret_name: str
    bootstrap_servers: str
    topic: str

    @staticmethod
    def from_env() -> "AppConfig":
        """Load configuration from environment variables with defaults/validation."""
        load_dotenv()

        aws_region = os.getenv("AWS_REGION", "us-east-1")
        kafka_secret_name = os.getenv("KAFKA_SECRET_NAME")  # kafka/credentials
        bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS")  # broker:9092
        topic = os.getenv("TOPIC", "transactions-stream")

        missing = [k for k, v in {
            "KAFKA_SECRET_NAME": kafka_secret_name,
            "BOOTSTRAP_SERVERS": bootstrap_servers,
        }.items() if not v]

        if missing:
            raise RuntimeError(
                f"Missing required configuration: {', '.join(missing)}. "
                "Check your .env or environment variables."
            )

        return AppConfig(
            aws_region=aws_region,
            kafka_secret_name=kafka_secret_name,
            bootstrap_servers=bootstrap_servers,
            topic=topic,
        )

# --------------------------- Secrets Manager --------------------------- #
def fetch_secret(secret_name: str, region_name: str) -> Dict[str, Any]:
    """
    Fetch and return a JSON secret from AWS Secrets Manager.

    The secret can be stored as either SecretString (JSON) or SecretBinary (base64).
    """
    client = boto3.client("secretsmanager", region_name=region_name)
    resp = client.get_secret_value(SecretId=secret_name)

    if "SecretString" in resp and resp["SecretString"]:
        return json.loads(resp["SecretString"])

    if "SecretBinary" in resp and resp["SecretBinary"]:
        return json.loads(b64decode(resp["SecretBinary"]).decode("utf-8"))

    raise RuntimeError(f"Secret {secret_name!r} does not contain usable data.")

# --------------------------- Kafka Producer --------------------------- #
def build_producer(bootstrap_servers: str, username: str, password: str) -> Producer:
    """Construct and return a configured Confluent Kafka Producer."""
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": username,
        "sasl.password": password,
        "client.id": "tx-producer",
        # Optional: tune queue.buffering.max.messages, linger.ms, etc.
    }
    return Producer(conf)

def delivery_report(err, msg) -> None:
    """Per-message delivery callback."""
    if err is not None:
        log.error("❌ Delivery failed: %s", err)
    else:
        log.info(
            "✅ Delivered to %s [%s] @ offset %s",
            msg.topic(), msg.partition(), msg.offset()
        )

# --------------------------- Event Generation --------------------------- #
CITY_STATE_CHOICES: Tuple[Tuple[str, str], ...] = (
    ("Littleton", "CO"),
    ("Austin", "TX"),
    ("Miami", "FL"),
    ("Denver", "CO"),
    ("Phoenix", "AZ"),
)

def make_event() -> Dict[str, Any]:
    """Create a single synthetic transaction event."""
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    city, state = random.choice(CITY_STATE_CHOICES)

    return {
        "trans_num": uuid.uuid4().hex,
        "trans_date_trans_time": now,
        "amt": round(random.uniform(1.0, 500.0), 2),
        "is_fraud": 0,
        "category": random.choice(["GROCERY", "SHOPPING", "GAS_TRANSPORT"]),
        "channel": random.choice(["POS", "NET"]),
        "city": city,
        "state": state,
        "city_pop": random.randint(5_000, 1_000_000),
        "lat": round(random.uniform(26.0, 48.0), 6),
        "long": round(random.uniform(-123.0, -71.0), 6),
        "dob": random.choice(["1982-01-15", "1978-06-20", "1990-11-03", "1986-03-22"]),
        "job": random.choice(["Engineer", "Teacher", "Manager", "Analyst"]),
        "merch_lat": round(random.uniform(26.0, 48.0), 6),
        "merch_long": round(random.uniform(-123.0, -71.0), 6),
    }

# --------------------------- CLI / Main --------------------------- #
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Produce synthetic transaction events to a Kafka topic."
    )
    parser.add_argument(
        "-n", "--count",
        type=int,
        default=5,
        help="Number of events to produce (default: 5)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.5,
        help="Delay (seconds) between messages (default: 0.5)",
    )
    return parser.parse_args()

def main() -> None:
    args = parse_args()

    # Load config
    cfg = AppConfig.from_env()
    log.info("🚀 Producing %d event(s) to topic '%s' ...", args.count, cfg.topic)

    # Fetch secrets
    secret = fetch_secret(cfg.kafka_secret_name, cfg.aws_region)
    try:
        username = secret["sasl_username"]
        password = secret["sasl_password"]
    except KeyError as ke:
        raise RuntimeError(
            f"Secret '{cfg.kafka_secret_name}' is missing key: {ke}. "
            "Expected keys: 'sasl_username', 'sasl_password'."
        ) from ke

    # Build producer
    producer = build_producer(cfg.bootstrap_servers, username, password)

    # Produce loop
    try:
        for _ in range(args.count):
            evt = make_event()
            producer.produce(
                cfg.topic,
                key=evt["trans_num"],
                value=json.dumps(evt, separators=(",", ":"), sort_keys=True),
                callback=delivery_report,
            )
            # Serve delivery callbacks
            producer.poll(0)
            time.sleep(args.delay)
    except KeyboardInterrupt:
        log.warning("Interrupted by user. Attempting to flush pending messages...")
    finally:
        # Ensure all messages are delivered (timeout in seconds)
        outstanding = producer.flush(timeout=10)
        if outstanding > 0:
            log.warning("⚠️ %d message(s) may not have been delivered.", outstanding)

if __name__ == "__main__":
    main()
