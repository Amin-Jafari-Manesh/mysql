import logging
import time
from os import environ
import mysql.connector
from mysql.connector import Error
from datetime import datetime

logging.basicConfig(level=logging.INFO)

db_config = {
    'PASS': environ.get('PASS', ''),
    'DOMAIN': environ.get('DOMAIN', ''),
    'DATA_TYPE': environ.get('DATA_TYPE', ''),
    'DATA_SIZE': int(environ.get('DATA_SIZE', 1)),
    'RECORDS': int(environ.get('RECORDS', 100)),
    'INSERT_DELAY': int(environ.get('INSERT_DELAY', 0)),
}

def connect_to_database():
    try:
        conn = mysql.connector.connect(
            host=db_config['DOMAIN'],
            user='admin',
            password=db_config['PASS'],
            database='db',
            port='3306'
        )
        if conn.is_connected():
            logging.info("Connected to the database.")
            return conn
    except Error as e:
        logging.error(f"Error connecting to the database: {e}")
    return None

def generate_random_hash(numb: int) -> str:
    import random
    import string
    import hashlib
    return ''.join(
        [hashlib.sha256(''.join(random.choices(string.ascii_letters + string.digits, k=64)).encode()).hexdigest()
         for _ in range(numb)])

def generate_text(numb: int) -> str:
    text = ' The quick brown fox jumps over the lazy dog today. '
    return ''.join([text for _ in range(numb)])

def mysql_write_data(conn, size: int) -> bool:
    if not conn or not conn.is_connected():
        logging.error("Not connected to the database.")
        return False

    try:
        cur = conn.cursor()
        table_name = ''
        if db_config['DATA_TYPE'] == 'h':
            table_name = 'hashes'
            cur.execute("CREATE TABLE IF NOT EXISTS hashes (id INT AUTO_INCREMENT PRIMARY KEY, hash TEXT, created_at TIMESTAMP);")
        elif db_config['DATA_TYPE'] == 't':
            table_name = 'text'
            cur.execute("CREATE TABLE IF NOT EXISTS text (id INT AUTO_INCREMENT PRIMARY KEY, texts TEXT, created_at TIMESTAMP);")
        else:
            logging.error("Invalid data type.")
            return False
        
        conn.commit()
        
        for i in range(size):
            start_time = time.time()
            if db_config['DATA_TYPE'] == 'h':
                data = generate_random_hash(db_config['DATA_SIZE'])
                cur.execute(f"INSERT INTO {table_name} (hash, created_at) VALUES (%s, %s)", (data, datetime.now()))
            else:
                data = generate_text(db_config['DATA_SIZE'])
                cur.execute(f"INSERT INTO {table_name} (texts, created_at) VALUES (%s, %s)", (data, datetime.now()))
            
            conn.commit()
            end_time = time.time()
            logging.info(f"Insert execution time: {end_time - start_time:.4f} seconds") 
            time.sleep(db_config['INSERT_DELAY'] * 0.001)
    except Error as e:
        logging.error(f"Database error: {e}")
        return False
    finally:
        if cur:
            cur.close()

    return True

def main():
    conn = connect_to_database()
    if conn:
        if mysql_write_data(conn, db_config['RECORDS']):
            logging.info("Data successfully written to the database.")
        else:
            logging.error("Failed to write data to the database.")
        conn.close()
        logging.info("Database connection closed.")
    else:
        logging.error("Failed to connect to the database.")

if __name__ == '__main__':
    main()
