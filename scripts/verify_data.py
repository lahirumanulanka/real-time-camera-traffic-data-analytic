
import psycopg2
from datetime import datetime
import logging

# --- Configure Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Database Connection Details ---
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "trafficdb"
DB_USER = "trafficuser"
DB_PASS = "trafficpass"

def verify_data():
    """Connects to the database and fetches sample data and range stats."""
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        cursor = conn.cursor()
        # Query to check for data in hourly_avg_volume
        query = "SELECT id, device_id, report_hour, avg_volume FROM hourly_avg_volume ORDER BY report_hour DESC LIMIT 10;"
        cursor.execute(query)
        results = cursor.fetchall()
        
        if results:
            logger.info("Successfully retrieved data from 'hourly_avg_volume' table:")
            print("--- Sample Data (device_id, report_hour, avg_volume) ---")
            for row in results:
                print(row)
        else:
            logger.warning("No data found in 'hourly_avg_volume' table.")

        # Range check aligned with Grafana time window
        range_from = datetime(2024, 6, 5, 0, 0)
        range_to = datetime(2024, 6, 5, 23, 59, 59)
        count_query = """
            SELECT COUNT(*)
            FROM hourly_avg_volume
            WHERE report_hour BETWEEN %s AND %s;
        """
        cursor.execute(count_query, (range_from, range_to))
        count = cursor.fetchone()[0]
        print(f"Rows in Grafana range [{range_from} .. {range_to}]: {count}")
            
        cursor.close()
    except psycopg2.OperationalError as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        if conn:
            conn.close()
            logger.info("PostgreSQL connection closed.")

if __name__ == "__main__":
    verify_data()
