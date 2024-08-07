import logging
from datetime import datetime
import time
from os import environ
import mysql.connector

logging.basicConfig(level=logging.INFO)

db_config = {
    'PASS': environ.get('PASS', ''),
    'DOMAIN': environ.get('DOMAIN', ''),
    'DATA_TYPE' : environ.get('DATA_TYPE', ''),
    'DATA_SIZE': int(environ.get('DATA_SIZE', '')),
    'RECORDS': int(environ.get('RECORDS', '')),
    'INSERT_DELAY' : int(environ.get('INSERT_DELAY', '')),
}

conn = mysql.connector.connect(
            host=db_config['DOMAIN'],
            user='admin',
            password=db_config['PASS'],
            database='db',
            port='3306'
        )

def generate_random_hash(numb: int ) -> str:
    import random
    import string
    import hashlib
    return ''.join(
        [hashlib.sha256(''.join(random.choices(string.ascii_letters + string.digits, k=64)).encode()).hexdigest()
            for _ in range(numb)])


def generate_text(numb: int) -> str:
    text = ' The quick brown fox jumps over the lazy dog today. '
    return ''.join([text for _ in range(numb)])


def mysql_write_hash(size: int) -> bool:
    if conn.is_connected():
        logging.info("Connected to the database.")
        cur = conn.cursor()
        if db_config['DATA_TYPE'] == 'h':
            cur.execute("CREATE TABLE IF NOT EXISTS hashes (id serial PRIMARY KEY, hash TEXT, created_at TIMESTAMP);")
            conn.commit()
            for _ in range(size):
                time.sleep(db_config['INSERT_DELAY']*0.001)
                cur.execute(f"INSERT INTO hashes (hash, created_at) VALUES ('{generate_random_hash(db_config['DATA_SIZE'])}', '{datetime.now()}')")
                conn.commit()
            conn.close()
        elif db_config['DATA_TYPE'] == 't':
            cur.execute("CREATE TABLE IF NOT EXISTS text (id serial PRIMARY KEY, texts TEXT, created_at TIMESTAMP);")
            conn.commit()
            for _ in range(size):
                time.sleep(db_config['INSERT_DELAY']*0.001)
                cur.execute(f"INSERT INTO text (texts, created_at) VALUES ('{generate_text(db_config['DATA_SIZE'])}', '{datetime.now()}')")
                conn.commit()
            conn.close()
        else:
            logging.error("Invalid data type.")
            return False
    else:
        logging.error("Failed to connect to the database.")
        return False
    return True

if __name__ == '__main__':
    if mysql_write_hash(db_config['RECORDS']):
        logging.info("Hashes successfully written to the database.")
    else:
        logging.error("Failed to write hashes to the database.")
