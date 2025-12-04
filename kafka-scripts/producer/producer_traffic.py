import orjson
from kafka import KafkaProducer
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """
    Main function to produce traffic data to Kafka.
    """
    # 1. Load JSON file
    json_file_path = 'data/dataset/traffic_cleaned.json'
    try:
        with open(json_file_path, 'rb') as f:
            traffic_data = orjson.loads(f.read())
    except FileNotFoundError:
        logger.error(f"Error: The file {json_file_path} was not found.")
        return
    except Exception as e:
        logger.error(f"Error loading JSON file: {e}")
        return

    # 2. Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=orjson.dumps
    )

    topic = 'traffic_raw'

    # 3. Sending messages
    for record in traffic_data:
        # Ensure timestamp field is named "read_time"
        if 'read_date' in record:
            record['read_time'] = record['read_date']
            # del record['read_date'] # Optional: remove old key

        # Send the entire dictionary to Kafka
        producer.send(topic, record)

        # Print log line
        if 'device_id' in record and 'read_time' in record:
            logger.info(f"Sent: device_id={record['device_id']}, read_time={record['read_time']}")

        # Sleep 0.1 seconds
        time.sleep(0.1)

    # 4. End
    producer.flush()
    producer.close()
    logger.info("Finished sending all records. Producer closed.")

if __name__ == "__main__":
    main()
