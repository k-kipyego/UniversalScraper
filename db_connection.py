import os
import json
import logging
from typing import List, Dict
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database connection parameters
DB_PARAMS = {
    'host': os.getenv('POSTGRES_HOST'),
    'port': os.getenv('POSTGRES_PORT'),
    'dbname': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD')
}

# Predefined labels (update this list as needed)
PREDEFINED_LABELS = [
    "Title", "Description", "Date", "Deadline", "Reference Number",
    "Category", "Location", "Organization", "Contact", "Value"
]

def get_db_connection():
    """Establishes and returns a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        logger.info("Successfully connected to the PostgreSQL database.")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to the PostgreSQL database: {e}")
        raise

def create_structured_table(conn, table_name: str):
    """Creates a new table with columns for predefined labels, website name, and URL."""
    columns = ", ".join([f"{label.lower().replace(' ', '_')} TEXT" for label in PREDEFINED_LABELS])
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        website_name TEXT,
        website_url TEXT,
        {columns},
        original_id INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(create_table_query)
            conn.commit()
            logger.info(f"Table '{table_name}' is ready.")
    except Exception as e:
        logger.error(f"Failed to create table '{table_name}': {e}")
        conn.rollback()
        raise

def fetch_scraped_data(conn) -> List[Dict]:
    """Fetches all data from the scraped_data table."""
    query = "SELECT id, data, website_name, website_url FROM scraped_data;"
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            data = cursor.fetchall()
            logger.info(f"Fetched {len(data)} rows from scraped_data table.")
            return data
    except Exception as e:
        logger.error(f"Failed to fetch data from scraped_data table: {e}")
        raise

def process_and_insert_data(conn, scraped_data: List[Dict], target_table: str):
    """Processes scraped data and inserts it into the structured table."""
    insert_query = f"""
    INSERT INTO {target_table} 
    (website_name, website_url, {', '.join([label.lower().replace(' ', '_') for label in PREDEFINED_LABELS])}, original_id)
    VALUES (%s, %s, {', '.join(['%s' for _ in PREDEFINED_LABELS])}, %s)
    """
    inserted_count = 0
    try:
        with conn.cursor() as cursor:
            for row in scraped_data:
                try:
                    json_data = json.loads(row['data']) if isinstance(row['data'], str) else row['data']
                    
                    # Extract listings if present
                    listings = json_data.get('listings', [json_data])
                    
                    for listing in listings:
                        values = [row['website_name'], row['website_url']]
                        values.extend([listing.get(label, '') for label in PREDEFINED_LABELS])
                        values.append(row['id'])  # Add original_id
                        cursor.execute(insert_query, values)
                        inserted_count += 1
                except json.JSONDecodeError as json_error:
                    logger.error(f"JSON parsing error for row {row['id']}: {json_error}")
                except Exception as e:
                    logger.error(f"Error processing row {row['id']}: {e}")
            
            conn.commit()
            logger.info(f"Successfully processed and inserted {inserted_count} rows into {target_table}")
    except Exception as e:
        logger.error(f"Failed to process and insert data: {e}")
        conn.rollback()
        raise

def main():
    target_table = 'structured_scraped_data'
    conn = None
    try:
        conn = get_db_connection()
        create_structured_table(conn, target_table)
        scraped_data = fetch_scraped_data(conn)
        if not scraped_data:
            logger.warning("No data found in the scraped_data table.")
        else:
            process_and_insert_data(conn, scraped_data, target_table)
        logger.info("Data processing and insertion completed.")
    except Exception as e:
        logger.error(f"An error occurred during the process: {e}")
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")

if __name__ == "__main__":
    main()