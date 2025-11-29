#!/usr/bin/env python3
import os
import time
import signal
import json
from datetime import datetime, timezone
from confluent_kafka import Consumer, KafkaException, OFFSET_END
from pymongo import MongoClient, errors as mongo_errors
import pyflink

# === CONFIG - replace these with your values ===
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "pkc-xxxxx.us-west-2.confluent.cloud:9092")
SASL_USERNAME = os.getenv("SASL_USERNAME", "YOUR_API_KEY")
SASL_PASSWORD = os.getenv("SASL_PASSWORD", "YOUR_API_SECRET")
SASL_MECHANISM = os.getenv("SASL_MECHANISM", "PLAIN")
SECURITY_PROTOCOL = os.getenv("SECURITY_PROTOCOL", "SASL_SSL")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "user")
GROUP_ID = os.getenv("GROUP_ID", "email-window-consumer-group")
AUTO_OFFSET_RESET = os.getenv("AUTO_OFFSET_RESET", "earliest")  # or latest

MONGO_URI = os.getenv("MONGO_URI", "mongodb://username:password@mongo-host:27017")
MONGO_DB = os.getenv("MONGO_DB", "kafka_windows")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "unique_emails")

WINDOW_SECONDS = int(os.getenv("WINDOW_SECONDS", "60"))  # 1 minute

# === Set up graceful shutdown ===
run = True
def handle_sigterm(signum, frame):
    global run
    run = False

signal.signal(signal.SIGINT, handle_sigterm)
signal.signal(signal.SIGTERM, handle_sigterm)

# === Create Mongo client ===
try:
    mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    mongo_db = mongo_client[MONGO_DB]
    mongo_collection = mongo_db[MONGO_COLLECTION]
    # Try server selection to surface connection errors early
    mongo_client.server_info()
except mongo_errors.PyMongoError as e:
    print("Failed to connect to MongoDB:", e)
    raise SystemExit(1)

# === Kafka consumer config ===
conf = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'sasl.mechanisms': SASL_MECHANISM,
    'security.protocol': SECURITY_PROTOCOL,
    'sasl.username': SASL_USERNAME,
    'sasl.password': SASL_PASSWORD,
    'group.id': GROUP_ID,
    'auto.offset.reset': AUTO_OFFSET_RESET,
    # enable auto commit? we'll commit manually for safety
    'enable.auto.commit': False,
}

consumer = Consumer(conf)

def flatten_email_from_message(msg_value):
    """
    Given message value (bytes/str), parse JSON and extract the email
    This matches your Java producer body: {"value": {"type":"JSON","data": "<email>"}}
    """
    try:
        if isinstance(msg_value, bytes):
            s = msg_value.decode('utf-8')
        else:
            s = str(msg_value)
        payload = json.loads(s)
        # Try to pull email from structure
        # Accept either top-level "value" or direct string
        if isinstance(payload, dict) and 'value' in payload:
            value = payload['value']
            if isinstance(value, dict) and 'data' in value:
                return value['data']
        # fallback: if payload is directly an email string
        if isinstance(payload, str):
            return payload
    except Exception:
        pass
    return None

def persist_window(window_start_ts, emails_set):
    """Persist the window summary into MongoDB as a single document (upsert optional)."""
    if not emails_set:
        return
    doc = {
        "window_start": window_start_ts.isoformat(),
        "window_seconds": WINDOW_SECONDS,
        "unique_emails": list(emails_set),
        "count": len(emails_set),
        "ingested_at": datetime.now(timezone.utc).isoformat()
    }
    try:
        # insert as new document
        mongo_collection.insert_one(doc)
        print(f"Persisted window {window_start_ts} with {len(emails_set)} emails.")
    except mongo_errors.PyMongoError as e:
        print("MongoDB insert failed:", e)

def run_consumer():
    try:
        consumer.subscribe([KAFKA_TOPIC])
        print(f"Subscribed to topic {KAFKA_TOPIC}. Starting consumption...")
        window_start = None
        window_emails = set()
        last_commit_ts = time.time()
        while run:
            
            msg = consumer.poll(1.0)  # seconds
            now = time.time()
            if msg is None:
                # check if window elapsed even if no message arrived
                if window_start is not None and now - window_start >= WINDOW_SECONDS:
                    # flush
                    window_start_ts = datetime.fromtimestamp(window_start, timezone.utc)
                    persist_window(window_start_ts, window_emails)
                    # commit offsets
                    try:
                        consumer.commit(asynchronous=False)
                    except KafkaException as e:
                        print("Commit failed:", e)
                    # reset
                    window_start = None
                    window_emails = set()
                continue
            if msg.error():
                # handle partition EOF and other errors
                print("Message error:", msg.error())
                continue

            # On first message set window start
            if window_start is None:
                window_start = now

            # parse message
            try:
                value = msg.value()
                email = flatten_email_from_message(value)
                if email:
                    window_emails.add(email)
                else:
                    print("Could not parse email from message:", value)
            except Exception as e:
                print("Failed to process message:", e)

            # If window elapsed, persist and reset
            if now - window_start >= WINDOW_SECONDS:
                window_start_ts = datetime.fromtimestamp(window_start, timezone.utc)
                persist_window(window_start_ts, window_emails)
                # commit offsets after persisting window
                try:
                    consumer.commit(asynchronous=False)
                except KafkaException as e:
                    print("Commit failed:", e)
                window_start = None
                window_emails = set()

        # loop ended -> final flush before exit
        if window_start and window_emails:
            window_start_ts = datetime.fromtimestamp(window_start, timezone.utc)
            persist_window(window_start_ts, window_emails)
            try:
                consumer.commit(asynchronous=False)
            except KafkaException as e:
                print("Commit failed:", e)

    except Exception as e:
        print("Fatal consumer exception:", e)
    finally:
        print("Closing consumer")
        consumer.close()
        try:
            mongo_client.close()
        except Exception:
            pass

if __name__ == "__main__":
    run_consumer()
