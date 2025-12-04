import psycopg2
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Database Connection Details ---
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "trafficdb"
DB_USER = "trafficuser"
DB_PASS = "trafficpass"

# --- SQL CREATE TABLE Statements ---
CREATE_HOURLY_AVG_VOLUME = """
CREATE TABLE IF NOT EXISTS hourly_avg_volume (
    id SERIAL PRIMARY KEY,
    device_id VARCHAR(255) NOT NULL,
    report_hour TIMESTAMP NOT NULL,
    avg_volume NUMERIC(10, 2),
    UNIQUE(device_id, report_hour)
);
"""

CREATE_DAILY_PEAK_VOLUME = """
CREATE TABLE IF NOT EXISTS daily_peak_volume (
    id SERIAL PRIMARY KEY,
    device_id VARCHAR(255) NOT NULL,
    report_date DATE NOT NULL,
    peak_volume INTEGER,
    UNIQUE(device_id, report_date)
);
"""

CREATE_DAILY_SENSOR_AVAILABILITY = """
CREATE TABLE IF NOT EXISTS daily_sensor_availability (
    id SERIAL PRIMARY KEY,
    device_id VARCHAR(255) NOT NULL,
    report_date DATE NOT NULL,
    availability_pct NUMERIC(5, 2),
    UNIQUE(device_id, report_date)
);
"""

TABLES = {
    "hourly_avg_volume": CREATE_HOURLY_AVG_VOLUME,
    "daily_peak_volume": CREATE_DAILY_PEAK_VOLUME,
    "daily_sensor_availability": CREATE_DAILY_SENSOR_AVAILABILITY,
}

def main():
    """
    Connects to PostgreSQL and creates the necessary tables for traffic metrics.
    """
    conn = None
    cursor = None
    try:
        # Establish connection
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        conn.autocommit = True
        cursor = conn.cursor()
        logger.info("Successfully connected to PostgreSQL.")

        # Create each table
        for table_name, create_statement in TABLES.items():
            cursor.execute(create_statement)
            logger.info(f"Table '{table_name}' created or already exists.")

    except psycopg2.OperationalError as e:
        logger.error(f"Connection Error: Could not connect to PostgreSQL. Please ensure the database is running and accessible.")
        logger.error(e)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
    finally:
        # Cleanup
        if cursor:
            cursor.close()
            logger.info("Cursor closed.")
        if conn:
            conn.close()
            logger.info("Connection closed.")

if __name__ == "__main__":
    main()
