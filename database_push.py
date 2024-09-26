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
    Creates a table in the PostgreSQL database if it doesn't already exist.
    
    Args:
        conn: Active PostgreSQL connection object.
        table_name: Name of the table to create.
    """
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        file_name TEXT,
        data JSONB NOT NULL,
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

def insert_json_data(conn, table_name: str, file_name: str, json_data: Any):
    """
    Inserts JSON data into the specified PostgreSQL table.
    
    Args:
        conn: Active PostgreSQL connection object.
        table_name: Name of the table where data will be inserted.
        file_name: Name of the JSON file.
        json_data: The JSON data to insert.
    """
    try:
        with conn.cursor() as cursor:
            insert_query = f"""
            INSERT INTO {table_name} (file_name, data)
            VALUES (%s, %s)
            """
            cursor.execute(insert_query, (file_name, Json(json_data)))
            conn.commit()
            logger.info(f"Data from '{file_name}' inserted successfully into '{table_name}'.")
    except Exception as e:
        logger.error(f"Failed to insert data from '{file_name}' into '{table_name}': {e}")
        conn.rollback()
        raise

def push_json_to_db(json_file_path: str, table_name: str = 'scraped_data'):
    """
    Reads a JSON file and pushes its content to the PostgreSQL database.
    
    Args:
        json_file_path: Path to the JSON file.
        table_name: Target table name in the database.
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
        insert_json_data(conn, table_name, file_name, json_data)
        
    except Exception as e:
        logger.exception(f"An error occurred while pushing JSON to DB: {e}")
        raise
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            logger.info("Database connection closed.")

def push_website_info_to_db(url, name, labels, table_name='website_info'):
    """
    Push website information to a separate table in the database.
    
    Args:
    url (str): The URL of the scraped website
    name (str): The name of the website
    labels (list): List of labels for the scraped content
    table_name (str): Name of the table to insert the data into
    """
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=os.getenv('POSTGRES_NAME'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            host=os.getenv('POSTGRES_HOST'),
            port=os.getenv('POSTGRES_PORT')
        )
        cur = conn.cursor()

        # Convert labels list to CSV string
        labels_csv = ','.join(labels)

        # Insert the website information
        cur.execute(
            f"INSERT INTO {table_name} (url, name, labels) VALUES (%s, %s, %s)",
            (url, name, labels_csv)
        )

        conn.commit()
        logger.info(f"Website information successfully inserted into {table_name}")

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