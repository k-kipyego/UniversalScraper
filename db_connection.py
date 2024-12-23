import os
import json
import logging
from typing import List, Dict
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from psycopg2.extras import RealDictRow

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
   "Title", "Description", "Date Posted", "Deadline", "Reference Number",
    "Category", "Location", "Language", "Contact", "Budget", "Type"
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
        direct_url TEXT,  -- Added direct URL column
        {columns},
        original_id INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (title, description, reference_number)
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
            
            # Add detailed debugging for the first row
            if data:
                logger.debug("Sample of first row:")
                logger.debug(f"Keys: {data[0].keys()}")
                logger.debug(f"Data field content type: {type(data[0].get('data'))}")
                logger.debug(f"Data field content: {data[0].get('data')[:500]}...")  # First 500 chars
            
            return data
    except Exception as e:
        logger.error(f"Failed to fetch data from scraped_data table: {e}")
        raise

def process_and_insert_data(conn, scraped_data: List[RealDictRow], target_table: str):
    """Processes scraped data and inserts into the structured table."""
    insert_query = f"""
    INSERT INTO {target_table} 
    (website_name, website_url, direct_url, {', '.join([label.lower().replace(' ', '_') for label in PREDEFINED_LABELS])}, original_id)
    VALUES (%s, %s, %s, {', '.join(['%s' for _ in PREDEFINED_LABELS])}, %s)
    ON CONFLICT (title, description, reference_number) DO NOTHING
    """
    
    inserted_count = 0
    skipped_count = 0
    
    try:
        with conn.cursor() as cursor:
            for row in scraped_data:
                try:
                    row_dict = dict(row)
                    data_content = row_dict.get('data')
                    if not data_content:
                        continue
                    
                    if isinstance(data_content, str):
                        try:
                            data_content = json.loads(data_content)
                        except json.JSONDecodeError:
                            continue
                    
                    listings = []
                    if isinstance(data_content, list):
                        for item in data_content:
                            if isinstance(item, dict) and 'listings' in item:
                                listings.extend(item.get('listings', []))
                    elif isinstance(data_content, dict) and 'listings' in data_content:
                        listings = data_content.get('listings', [])
                    
                    website_name = row_dict.get('website_name', 'Unknown')
                    website_url = row_dict.get('website_url', 'Unknown')
                    
                    for listing in listings:
                        if isinstance(listing, (dict, RealDictRow)):
                            listing_dict = dict(listing) if isinstance(listing, RealDictRow) else listing
                            
                            # Extract direct_url from listing or fall back to website_url
                            direct_url = listing_dict.get('direct_url', website_url)
                            
                            values = [website_name, website_url, direct_url]
                            values.extend([listing_dict.get(label, '') for label in PREDEFINED_LABELS])
                            values.append(row_dict.get('id', None))
                            
                            cursor.execute(insert_query, values)
                            
                            if cursor.rowcount > 0:
                                inserted_count += 1
                            else:
                                skipped_count += 1
                            
                except Exception as e:
                    logger.error(f"Error processing entry: {e}", exc_info=True)
                    continue
            
            conn.commit()
            logger.info(f"Inserted {inserted_count} new rows, skipped {skipped_count} rows")
    except Exception as e:
        logger.error(f"Failed to merge and insert data: {e}", exc_info=True)
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
        logger.info("Data merging and insertion completed.")
    except Exception as e:
        logger.error(f"An error occurred during the process: {e}")
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")

if __name__ == "__main__":
    main()