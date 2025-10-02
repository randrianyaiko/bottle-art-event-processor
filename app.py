from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json
from concurrent.futures import ThreadPoolExecutor
from dateutil import parser
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
import os
import time
import logging
from src.event_processor.processor import event_handler

# -------------------------
# Setup
# -------------------------
load_dotenv()

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# Kafka and consumer settings
TOPIC = os.getenv("KAFKA_TOPIC")
GROUP_ID = os.getenv("KAFKA_GROUP_ID")
LOCAL_TZ = ZoneInfo(os.getenv("TIMEZONE"))
KAFKA_BROKERS = f'{os.getenv("KAFKA_HOST")}:{os.getenv("KAFKA_PORT")}'

# Thread pool for parallel processing
MAX_WORKERS = 10
executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

# Retry settings for event handling
MAX_EVENT_RETRIES = os.getenv("MAX_EVENT_RETRIES",10)
RETRY_DELAY = os.getenv("RETRY_DELAY", 3)


# -------------------------
# Helpers
# -------------------------
def normalize_event(event: dict) -> dict:
    """Normalize created_at to human-readable string in local timezone."""
    if "created_at" in event:
        try:
            dt = parser.isoparse(event["created_at"])
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=ZoneInfo("UTC"))
            dt_local = dt.astimezone(LOCAL_TZ)
            event["created_at"] = dt_local.strftime("%Y-%m-%d %H:%M:%S %Z")
        except Exception as e:
            logging.warning(
                f"Failed to normalize created_at ({event.get('created_at')}): {e}"
            )
    return event


def process_event_with_retries(event: dict) -> None:
    """Process an event with retries. Skip after MAX_EVENT_RETRIES failures."""
    for attempt in range(1, MAX_EVENT_RETRIES + 1):
        try:
            event_handler(event)
            logging.info(f"Event processed successfully on attempt {attempt}")
            return
        except Exception as e:
            logging.error(
                f"Event processing failed (attempt {attempt}/{MAX_EVENT_RETRIES}): {e}"
            )
            time.sleep(RETRY_DELAY)

    logging.error(f"Event permanently failed after {MAX_EVENT_RETRIES} retries. Skipping.")


def wait_for_kafka_connection():
    """Retry loop to wait for Kafka to be ready."""
    for attempt in range(20):
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_BROKERS,
                group_id=GROUP_ID,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
            logging.info("Kafka consumer connected!")
            return consumer
        except NoBrokersAvailable:
            logging.warning(
                f"Kafka not ready (attempt {attempt+1}/20), retrying in 5s..."
            )
            time.sleep(5)
    raise RuntimeError("Cannot connect to Kafka after multiple retries")


# -------------------------
# Main loop
# -------------------------
def main():
    consumer = wait_for_kafka_connection()
    futures = []

    try:
        for msg in consumer:
            try:
                logging.info(f"Received event: {msg.value}")
                event = normalize_event(msg.value)

                # Submit event processing to thread pool with retry logic
                future = executor.submit(process_event_with_retries, event)
                futures.append(future)

                # Clean up completed futures to avoid memory growth
                if len(futures) >= MAX_WORKERS:
                    futures = [f for f in futures if not f.done()]
                    for f in futures:
                        try:
                            f.result()
                        except Exception as e:
                            logging.error(f"Thread execution error: {e}")

            except Exception as e:
                logging.error(f"Failed to handle Kafka message: {e}")

    except KeyboardInterrupt:
        logging.info("Consumer stopped manually.")

    finally:
        consumer.close()
        executor.shutdown(wait=True)
        logging.info("Kafka consumer shut down.")


if __name__ == "__main__":
    main()
