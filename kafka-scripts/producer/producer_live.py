import time
import random
import logging
from datetime import datetime, timezone

import orjson
from kafka import KafkaProducer


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC = 'traffic_raw'

# A small set of demo sensors
DEVICE_IDS = ["7038", "7042", "7051", "7060", "7077"]


def create_producer() -> KafkaProducer:
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=orjson.dumps,
            linger_ms=10,
        )
        logger.info("Kafka Producer created successfully.")
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka Producer: {e}")
        raise


def make_message(device_id: str) -> dict:
    # Use current UTC time; include 'Z' suffix to avoid local/UTC drift
    now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    # Generate a plausible vehicle count per 15s sample
    volume = max(0, int(random.gauss(12, 5)))

    return {
        "device_id": device_id,
        "read_time": now_iso,
        "volume": float(volume),
    }


def run():
    producer = create_producer()
    try:
        logger.info("Starting continuous sensor streaming to topic '%s'", TOPIC)
        while True:
            for device_id in DEVICE_IDS:
                msg = make_message(device_id)
                producer.send(TOPIC, msg)
            producer.flush()
            # Emit new samples every 5 seconds across all devices
            time.sleep(5)
    except KeyboardInterrupt:
        logger.info("Producer interrupted by user.")
    finally:
        producer.close()
        logger.info("Kafka Producer closed.")


if __name__ == "__main__":
    run()
import time
import random
import logging
from datetime import datetime, timezone

import orjson
from kafka import KafkaProducer


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC = 'traffic_raw'

# A small set of demo sensors
DEVICE_IDS = ["7038", "7042", "7101", "7155"]


def create_producer() -> KafkaProducer:
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=orjson.dumps,
            linger_ms=10,
        )
        logger.info("Kafka Producer created successfully.")
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka Producer: {e}")
        raise


def make_message(device_id: str) -> dict:
    # Use current time; consumer expects 'read_time' ISO-8601
    now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    # Generate a plausible vehicle count per 15s sample
    # Range can be tuned to your dataset; keep simple for demo
    volume = max(0, int(random.gauss(12, 5)))

    return {
        "device_id": device_id,
        "read_time": now_iso,
        "volume": volume,
    }


def run():
    producer = create_producer()
    try:
        logger.info("Starting continuous sensor streaming to topic '%s'", TOPIC)
        while True:
            for device_id in DEVICE_IDS:
                msg = make_message(device_id)
                producer.send(TOPIC, msg)
            producer.flush()
            # Emit new samples every 5 seconds across all devices
            time.sleep(5)
    except KeyboardInterrupt:
        logger.info("Producer interrupted by user.")
    finally:
        producer.close()
        logger.info("Kafka Producer closed.")


if __name__ == "__main__":
    run()
