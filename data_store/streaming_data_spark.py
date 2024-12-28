import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

# Database connection details
DB_CONFIG = {
    "dbname": "binance_data",
    "user": "your_user",  # Replace with your PostgreSQL username
    "password": "your_password",  # Replace with your PostgreSQL password
    "host": "localhost",  # Use 'localhost' if accessing from host machine
    "port": 5432,  # PostgreSQL default port
}


# Function to convert milliseconds to ISO 8601 datetime
def convert_timestamp(milliseconds):
    return datetime.utcfromtimestamp(milliseconds / 1000)


# Function to insert a batch of data into the database
def write_to_db(batch):
    if not batch:  # No data to write
        print("Batch is empty. Skipping database insertion.")
        return

   
    INSERT_QUERY = """
        INSERT INTO trade_data.trades (
            event_type, event_time, symbol, trade_id, price, quantity, trade_time, buyer_maker, ignore
        )
        VALUES %s
    """

    # Prepare the batch for insertion
    formatted_batch = [
        (
            record.get("e"),  # Event type
            convert_timestamp(record.get("E")),  # Event time (converted)
            record.get("s"),  # Symbol
            record.get("t"),  # Trade ID
            float(record.get("p", 0)),  # Price
            float(record.get("q", 0)),  # Quantity
            convert_timestamp(record.get("T")),  # Trade time (converted)
            record.get("m", False),  # Buyer is market maker
            False,  # Ignore (default False)
        )
        for record in batch
    ]

    # Connect to the database and insert the data
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        execute_values(cursor, INSERT_QUERY, formatted_batch)
        conn.commit()
        print(f"Inserted {len(batch)} records into the database.")
    except Exception as e:
        print(f"Error inserting into database: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
