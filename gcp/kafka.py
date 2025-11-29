#!/usr/bin/env python3
import os
import time
import signal
import json
from datetime import datetime, timezone
from confluent_kafka import Consumer, Producer, KafkaException
from pymongo import MongoClient, errors as mongo_errors

# ============================================================
# CONFIG (Replace with your actual values or export as env vars)
# ============================================================
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "pkc-xxxxx.us-west-2.confluent.cloud:9092")
SASL_USERNAME = os.getenv("SASL_USERNAME", "YOUR_API_KEY")
SASL_PASSWORD = os.getenv("SASL_PASSWORD", "YOUR_API_SECRET")
SASL_MECHANISM = os.getenv("SASL_MECHANISM", "PLAIN")
SECURITY_PROTOCOL = os.getenv("SECURITY_PROTOCOL", "SASL_SSL")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "user")
GROUP_ID = os.getenv("GROUP_ID", "email-window-consumer-group")
AUTO_OFFSET_RESET = os.getenv("AUTO_OFFSET_RESET", "earliest")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://username:password@mongo-host:27017")
MONGO_DB = os.getenv("MONGO_DB", "kafka_windows")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "unique_emails")

WINDOW_SECONDS = int(os.getenv("WINDOW_SECONDS", "60"))

# ============================================================
# Graceful Shutdown
# ============================================================
run = True
def handle_sigterm(signum, frame):
    global run
    run = False

signal.signal(signal.SIGINT, handle_sigterm)
signal.signal(signal.SIGTERM, handle_sigterm)

# ============================================================
# MongoDB Setup
# ============================================================
try:
    mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    mongo_db = mongo_client[MONGO_DB]
    mongo_collection = mongo_db[MONGO_COLLECTION]
    mongo_client.server_info()
except mongo_errors.PyMongoError as e:
    print("Failed to connect to MongoDB:", e)
    raise SystemExit(1)

# ============================================================
# Kafka Consumer
# ============================================================
consumer_conf = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'sasl.mechanisms': SASL_MECHANISM,
    'security.protocol': SECURITY_PROTOCOL,
    'sasl.username': SASL_USERNAME,
    'sasl.password': SASL_PASSWORD,
    'group.id': GROUP_ID,
    'auto.offset.reset': AUTO_OFFSET_RESET,
    'enable.auto.commit': False,
}

consumer = Consumer(consumer_conf)

# ============================================================
# Kafka Producer (For Tombstones)
# ============================================================
producer = Producer({
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'sasl.mechanisms': SASL_MECHANISM,
    'security.protocol': SECURITY_PROTOCOL,
    'sasl.username': SASL_USERNAME,
    'sasl.password': SASL_PASSWORD,
})

# ============================================================
# Utility Functions
# ============================================================
def flatten_email_from_message(msg_value):
    """Extract email from JSON payload."""
    try:
        s = msg_value.decode('utf-8') if isinstance(msg_value, bytes) else str(msg_value)
        payload = json.loads(s)
        if isinstance(payload, dict) and 'value' in payload:
            if isinstance(payload['value'], dict) and 'data' in payload['value']:
                return payload['value']['data']
        if isinstance(payload, str):
            return payload
    except Exception:
        pass
    return None


def delete_kafka_message(email):
    """Send tombstone (null value) to compacted Kafka topic."""
    try:
        producer.produce(KAFKA_TOPIC, key=email, value=None)
        producer.flush()
        print(f"Tombstone sent → Kafka will delete: {email}")
    except Exception as e:
        print("Failed to send tombstone:", e)


def persist_window(window_start_ts, emails_set):
    """Delete old Mongo docs and insert a fresh window."""
    try:
        mongo_collection.delete_many({})   # wipe old records
        print("MongoDB cleared.")

        if not emails_set:
            print("No emails in this window.")
            return

        doc = {
            "window_start": window_start_ts.isoformat(),
            "window_seconds": WINDOW_SECONDS,
            "unique_emails": list(emails_set),
            "count": len(emails_set),
            "ingested_at": datetime.now(timezone.utc).isoformat()
        }

        mongo_collection.insert_one(doc)
        print(f"Inserted new window: {len(emails_set)} emails")

    except mongo_errors.PyMongoError as e:
        print("MongoDB write failed:", e)


# ============================================================
# Main Consumer Loop
# ============================================================
def run_consumer():
    try:
        consumer.subscribe([KAFKA_TOPIC])
        print(f"Subscribed to topic {KAFKA_TOPIC}")

        window_start = None
        window_emails = set()

        while run:
            msg = consumer.poll(1.0)
            now = time.time()

            if msg is None:
                # Check if window expired without new messages
                if window_start and now - window_start >= WINDOW_SECONDS:
                    ts = datetime.fromtimestamp(window_start, timezone.utc)
                    persist_window(ts, window_emails)
                    consumer.commit(asynchronous=False)
                    window_start = None
                    window_emails.clear()
                continue

            if msg.error():
                print("Message error:", msg.error())
                continue

            if window_start is None:
                window_start = now

            email = flatten_email_from_message(msg.value())
            if email:
                window_emails.add(email)
                delete_kafka_message(email)  # DELETE IMMEDIATELY
            else:
                print("Could not parse email:", msg.value())

            # Window complete?
            if now - window_start >= WINDOW_SECONDS:
                ts = datetime.fromtimestamp(window_start, timezone.utc)
                persist_window(ts, window_emails)
                consumer.commit(asynchronous=False)
                window_start = None
                window_emails.clear()

        # Final flush on exit
        if window_start and window_emails:
            ts = datetime.fromtimestamp(window_start, timezone.utc)
            persist_window(ts, window_emails)
            consumer.commit(asynchronous=False)

    finally:
        print("Shutting down...")
        consumer.close()
        mongo_client.close()


# ============================================================
# ENTRY POINT
# ============================================================
if __name__ == "__main__":
    run_consumer()
