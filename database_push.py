import os
import json
import logging
from typing import Any
import psycopg2
from psycopg2.extras import Json
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # Set to DEBUG for detailed logs
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("database_push.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Database connection parameters from environment variables
DB_PARAMS = {
    'host': os.getenv('POSTGRES_HOST'),
    'port': os.getenv('POSTGRES_PORT'),
    'dbname': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD')
}

def get_db_connection():
    """
    Establishes and returns a connection to the PostgreSQL database.
    """
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        logger.info("Successfully connected to the PostgreSQL database.")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to the PostgreSQL database: {e}")
        raise

def create_table(conn, table_name: str):
    """
    Creates the scraped_data table in the PostgreSQL database if it doesn't already exist.
    
    Args:
        conn: Active PostgreSQL connection object.
        table_name: Name of the table to create.
    """
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        file_name TEXT NOT NULL,
        data JSONB NOT NULL,
        website_name TEXT,  -- Added website name
        website_url TEXT,   -- Added website URL
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

def insert_json_data(conn, table_name: str, file_name: str, json_data: Any, website_name: str, website_url: str):
    """
    Inserts JSON data into the specified PostgreSQL table or updates existing data if new opportunities arise.
    
    Args:
        conn: Active PostgreSQL connection object.
        table_name: Name of the table where data will be inserted.
        file_name: Name of the JSON file.
        json_data: The JSON data to insert.
        website_name: The name of the website.
        website_url: The URL of the website.
    """
    try:
        logger.debug(f"Inserting data into {table_name}: file_name={file_name}, website_name={website_name}, website_url={website_url}")
        with conn.cursor() as cursor:
            # Check if the record already exists
            select_query = f"""
            SELECT data FROM {table_name} 
            WHERE website_name = %s AND website_url = %s
            """
            cursor.execute(select_query, (website_name, website_url))
            existing_data = cursor.fetchone()

            if existing_data:
                # Check for new opportunities in the new JSON data
                existing_opportunities = existing_data[0].get('opportunities', [])
                new_opportunities = json_data.get('opportunities', [])

                # Only update if there are new opportunities
                if not set(new_opportunities).issubset(existing_opportunities):
                    # Update the existing record by merging the new JSON data
                    update_query = f"""
                    UPDATE {table_name} 
                    SET data = data || %s 
                    WHERE website_name = %s AND website_url = %s
                    """
                    cursor.execute(update_query, (Json(json_data), website_name, website_url))
                    logger.info(f"Data from '{file_name}' updated successfully for '{website_name}'.")
                else:
                    logger.info(f"No new opportunities to update for '{website_name}'.")
            else:
                # Insert new record
                insert_query = f"""
                INSERT INTO {table_name} (file_name, data, website_name, website_url)
                VALUES (%s, %s, %s, %s)
                """
                cursor.execute(insert_query, (file_name, Json(json_data), website_name, website_url))
                logger.info(f"Data from '{file_name}' inserted successfully into '{table_name}'.")

            conn.commit()
    except Exception as e:
        logger.error(f"Failed to insert or update data from '{file_name}' into '{table_name}': {e}")
        conn.rollback()
        raise

def push_json_to_db(json_file_path: str, table_name: str = 'scraped_data', website_name: str = '', website_url: str = ''):
    """
    Reads a JSON file and pushes its content to the PostgreSQL database.
    
    Args:
        json_file_path: Path to the JSON file.
        table_name: Target table name in the database.
        website_name: The name of the website.
        website_url: The URL of the website.
    """
    try:
        # Check if file exists
        if not os.path.exists(json_file_path):
            logger.error(f"JSON file does not exist: {json_file_path}")
            raise FileNotFoundError(f"JSON file does not exist: {json_file_path}")
        
        # Check if file is not empty
        file_size = os.path.getsize(json_file_path)
        if file_size == 0:
            logger.error(f"JSON file is empty: {json_file_path}")
            raise ValueError(f"JSON file is empty: {json_file_path}")
        else:
            logger.debug(f"JSON file size: {file_size} bytes")
        
        # Establish database connection
        conn = get_db_connection()
        
        # Create table if it doesn't exist
        create_table(conn, table_name)
        
        # Read JSON data from the file
        with open(json_file_path, 'r', encoding='utf-8') as f:
            try:
                json_data = json.load(f)
                logger.debug(f"Successfully loaded JSON data from {json_file_path}")
            except json.JSONDecodeError as json_err:
                logger.error(f"JSON decoding failed for file {json_file_path}: {json_err}")
                raise
        
        # Extract file name from the file path
        file_name = os.path.basename(json_file_path)
        logger.debug(f"Extracted file name: {file_name}")
        
        # Insert the JSON data into the database
        insert_json_data(conn, table_name, file_name, json_data, website_name, website_url)        
    except Exception as e:
        logger.exception(f"An error occurred while pushing JSON to DB: {e}")
        raise
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            logger.info("Database connection closed.")

def create_website_info_table(conn, table_name: str):
    """
    Creates the website_info table in the PostgreSQL database if it doesn't already exist.
    
    Args:
        conn: Active PostgreSQL connection object.
        table_name: Name of the table to create.
    """
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        website_url TEXT NOT NULL,
        website_name TEXT NOT NULL,
        labels TEXT[] NOT NULL,  -- Ensure this column is defined as an array
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

def push_website_info_to_db(website_url, website_name, labels, table_name='website_info'):
    """
    Push website information to a separate table in the database.
    
    Args:
    website_url (str): The URL of the scraped website
    website_name (str): The name of the website
    labels (list): List of labels for the scraped content
    table_name (str): Name of the table to insert the data into
    """
    conn = None
    try:
        conn = get_db_connection()
        
        # Create the website_info table if it doesn't exist
        create_website_info_table(conn, table_name)

        cur = conn.cursor()

        # Convert labels list to PostgreSQL array format
        labels_array = '{' + ','.join(labels) + '}'

        # Check if the record already exists
        select_query = f"""
        SELECT labels FROM {table_name} 
        WHERE website_url = %s AND website_name = %s
        """
        cur.execute(select_query, (website_url, website_name))
        existing_data = cur.fetchone()

        if existing_data:
            # Update the existing record by merging the new labels
            update_query = f"""
            UPDATE {table_name} 
            SET labels = array_cat(labels, %s) 
            WHERE website_url = %s AND website_name = %s
            """
            cur.execute(update_query, (labels_array, website_url, website_name))
            logger.info(f"Website information updated successfully for '{website_name}'.")
        else:
            # Insert new record
            cur.execute(
                f"INSERT INTO {table_name} (website_url, website_name, labels) VALUES (%s, %s, %s)",
                (website_url, website_name, labels_array)
            )
            logger.info(f"Website information successfully inserted into {table_name}")

        conn.commit()

    except (Exception, psycopg2.Error) as error:
        logger.error(f"Error while connecting to PostgreSQL or inserting data: {error}", exc_info=True)
        raise

    finally:
        if conn:
            cur.close()
            conn.close()
    

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Push JSON data to PostgreSQL database.")
    parser.add_argument('json_file', help='Path to the JSON file to be inserted.')
    parser.add_argument('--table', default='scraped_data', help='Target table name in the database.')
    
    args = parser.parse_args()
    
    push_json_to_db(args.json_file, args.table)