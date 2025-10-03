from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
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
LOG_LEVEL = logging.DEBUG if os.getenv("DEBUG") == "1" else logging.INFO
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# Kafka and consumer settings
TOPIC = os.getenv("KAFKA_TOPIC")
GROUP_ID = os.getenv("KAFKA_GROUP_ID")
LOCAL_TZ = ZoneInfo(os.getenv("TIMEZONE"))
KAFKA_BROKERS = f'{os.getenv("KAFKA_HOST")}:{os.getenv("KAFKA_PORT")}'

# Thread pool for parallel processing (adaptive)
CPU_COUNT = os.cpu_count() or 1
MAX_WORKERS = min(4, CPU_COUNT * 2)  # lightweight on t2.micro, scales up on bigger servers
executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

# Retry settings
MAX_EVENT_RETRIES = int(os.getenv("MAX_EVENT_RETRIES", 3))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", 3))

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
            logging.debug(f"Event processed successfully on attempt {attempt}")
            return
        except Exception as e:
            logging.error(
                f"Event processing failed (attempt {attempt}/{MAX_EVENT_RETRIES}): {e}"
            )
            if attempt < MAX_EVENT_RETRIES:
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
                max_poll_records=MAX_WORKERS,  # prevent overload
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
                event = normalize_event(msg.value)
                logging.debug(f"Received event: {event}")

                # Submit event processing
                futures.append(executor.submit(process_event_with_retries, event))

                # Drain finished futures immediately (avoid memory bloat)
                for f in as_completed(futures, timeout=0):
                    try:
                        f.result()
                    except Exception as e:
                        logging.error(f"Thread execution error: {e}")
                    finally:
                        futures.remove(f)

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
