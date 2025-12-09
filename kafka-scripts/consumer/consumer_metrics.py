import logging
import orjson
from kafka import KafkaConsumer, KafkaProducer
import psycopg2
from datetime import datetime, timedelta
from collections import defaultdict

# --- Configure Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Kafka Configuration ---
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
CONSUMER_TOPIC = 'traffic_raw'
PRODUCER_TOPIC = 'traffic_metrics'

# --- Database Connection Details ---
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "trafficdb"
DB_USER = "trafficuser"
DB_PASS = "trafficpass"

# --- Metrics Constants ---
EXPECTED_BINS_PER_DAY = 96  # 24 hours * (60 / 15) bins

# --- Helper Functions ---
def create_consumer():
    """Creates and returns a Kafka consumer."""
    try:
        consumer = KafkaConsumer(
            CONSUMER_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest',
            value_deserializer=lambda m: orjson.loads(m.decode('utf-8'))
        )
        logger.info("Kafka Consumer created successfully.")
        return consumer
    except Exception as e:
        logger.error(f"Failed to create Kafka Consumer: {e}")
        raise

def create_producer():
    """Creates and returns a Kafka producer."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=orjson.dumps
        )
        logger.info("Kafka Producer created successfully.")
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka Producer: {e}")
        raise

def create_db_conn():
    """Creates and returns a PostgreSQL database connection."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        conn.autocommit = True
        logger.info("PostgreSQL connection created successfully.")
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        raise

def main():
    """
    Consumes traffic data, computes metrics, and stores/publishes them.
    """
    consumer = create_consumer()
    producer = create_producer()
    db_conn = create_db_conn()
    cursor = db_conn.cursor()

    # In-memory storage for metrics
    hourly_sum = defaultdict(float)
    hourly_count = defaultdict(int)
    daily_hour_total = defaultdict(float)
    daily_peak = defaultdict(float)
    seen_bins = defaultdict(set)

    logger.info("Starting to consume messages from topic: %s", CONSUMER_TOPIC)
    try:
        for message in consumer:
            data = message.value
            device_id = data.get('device_id')
            read_time_str = data.get('read_time')
            volume = data.get('volume', 0.0)

            if not all([device_id, read_time_str, volume is not None]):
                logger.warning("Skipping message with missing essential data: %s", data)
                continue

            if read_time_str.endswith('Z'):
                read_time_str = read_time_str[:-1]
            try:
                read_time = datetime.fromisoformat(read_time_str)
            except (ValueError, TypeError):
                logger.warning("Skipping message with invalid timestamp: %s", read_time_str)
                continue
            # --- Persist raw reading for real-time visualization ---
            try:
                cursor.execute(
                    """
                    INSERT INTO traffic_readings (device_id, read_time, volume)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (device_id, read_time) DO UPDATE SET volume = EXCLUDED.volume;
                    """,
                    (device_id, read_time, volume)
                )
            except Exception as e:
                logger.warning(f"Failed to insert raw reading: {e}")


            day_date = read_time.date()
            hour = read_time.hour
            minute = read_time.minute

            # --- Metric 1: Hourly average vehicle count per sensor ---
            hourly_key = (device_id, day_date, hour)
            hourly_sum[hourly_key] += volume
            hourly_count[hourly_key] += 1
            avg_volume = hourly_sum[hourly_key] / hourly_count[hourly_key]
            
            window_start = read_time.replace(minute=0, second=0, microsecond=0)
            window_end = window_start + timedelta(hours=1)

            # Publish to Kafka
            hourly_metric_msg = {
                "type": "hourly_avg_volume",
                "device_id": device_id,
                "window_start": window_start.isoformat(),
                "window_end": window_end.isoformat(),
                "avg_volume": avg_volume
            }
            producer.send(PRODUCER_TOPIC, hourly_metric_msg)

            # Upsert to PostgreSQL
            upsert_hourly_sql = """
                INSERT INTO hourly_avg_volume (device_id, report_hour, avg_volume)
                VALUES (%s, %s, %s)
                ON CONFLICT (device_id, report_hour) DO UPDATE SET avg_volume = EXCLUDED.avg_volume;
            """
            cursor.execute(upsert_hourly_sql, (device_id, window_start, avg_volume))
            logger.info(f"Upserted hourly average for {device_id} at {window_start}: {avg_volume:.2f}")

            # --- Metric 2: Daily peak traffic volume per sensor ---
            daily_key = (device_id, day_date)
            daily_hour_key = (device_id, day_date, hour)
            daily_hour_total[daily_hour_key] += volume
            current_hourly_total = daily_hour_total[daily_hour_key]

            if current_hourly_total > daily_peak[daily_key]:
                daily_peak[daily_key] = current_hourly_total
                
                # Publish to Kafka
                peak_metric_msg = {
                    "type": "daily_peak_volume",
                    "device_id": device_id,
                    "day_date": day_date.isoformat(),
                    "peak_volume": current_hourly_total
                }
                producer.send(PRODUCER_TOPIC, peak_metric_msg)

                # Upsert to PostgreSQL
                upsert_peak_sql = """
                    INSERT INTO daily_peak_volume (device_id, report_date, peak_volume)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (device_id, report_date) DO UPDATE SET peak_volume = EXCLUDED.peak_volume;
                """
                cursor.execute(upsert_peak_sql, (device_id, day_date, current_hourly_total))
                logger.info(f"Upserted daily peak for {device_id} on {day_date}: {current_hourly_total:.2f}")

            # --- Metric 3: Daily sensor availability (%) ---
            availability_key = (device_id, day_date)
            binned_minute = (minute // 15) * 15
            time_bin = (hour, binned_minute)
            
            if time_bin not in seen_bins[availability_key]:
                seen_bins[availability_key].add(time_bin)
                num_seen_bins = len(seen_bins[availability_key])
                availability_pct = (num_seen_bins / EXPECTED_BINS_PER_DAY) * 100
                
                # Publish to Kafka
                availability_msg = {
                    "type": "daily_sensor_availability",
                    "device_id": device_id,
                    "day_date": day_date.isoformat(),
                    "availability_pct": availability_pct
                }
                producer.send(PRODUCER_TOPIC, availability_msg)

                # Upsert to PostgreSQL
                upsert_availability_sql = """
                    INSERT INTO daily_sensor_availability (device_id, report_date, availability_pct)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (device_id, report_date) DO UPDATE SET availability_pct = EXCLUDED.availability_pct;
                """
                cursor.execute(upsert_availability_sql, (device_id, day_date, availability_pct))
                logger.info(f"Upserted availability for {device_id} on {day_date}: {availability_pct:.2f}%")
            
            producer.flush()

    except KeyboardInterrupt:
        logger.info("Consumer process interrupted by user.")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)
    finally:
        # Cleanup
        if consumer:
            consumer.close()
            logger.info("Kafka Consumer closed.")
        if producer:
            producer.close()
            logger.info("Kafka Producer closed.")
        if cursor:
            cursor.close()
        if db_conn:
            db_conn.close()
            logger.info("PostgreSQL connection closed.")

if __name__ == "__main__":
    main()
