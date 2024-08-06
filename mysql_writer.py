import logging
from datetime import datetime
from os import environ
import mysql.connector

logging.basicConfig(level=logging.INFO)

db_config = {
    'PASS': environ.get('PASS', ''),
    'DOMAIN': environ.get('DOMAIN', ''),
    'HASH_SIZE': int(environ.get('HASH_SIZE', '')),
    'RECORDS': int(environ.get('RECORDS', '')),
}


def generate_random_hash(numb: int = 1) -> str:
    import random
    import string
    import hashlib
    if numb == 1:
        return hashlib.sha256(''.join(random.choices(string.ascii_letters + string.digits, k=64)).encode()).hexdigest()
    else:
        return ''.join(
            [hashlib.sha256(''.join(random.choices(string.ascii_letters + string.digits, k=64)).encode()).hexdigest()
             for _ in range(numb)])


def test_mysql_connection():
    try:
        conn = mysql.connector.connect(
            host=db_config['DOMAIN'],
            user='admin',
            password=db_config['PASS'],
            database='db',
            port='3306'
        )
        logging.info("Successfully connected to MySQL")
        cur = conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS hashes (id serial PRIMARY KEY, hash TEXT, created_at TIMESTAMP);")
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        logging.error(f"MySQL connection failed: {e}")
        return False


def mysql_write_hash(size: int = 100) -> bool:
    if test_mysql_connection():
        conn = mysql.connector.connect(
            host=db_config['DOMAIN'],
            user='admin',
            password=db_config['PASS'],
            database='db',
            port='3306'
        )
        cur = conn.cursor()
        for _ in range(size):
            cur.execute(f"INSERT INTO hashes (hash, created_at) VALUES ('{generate_random_hash(db_config['HASH_SIZE'])}', '{datetime.now()}')")
            conn.commit()
        conn.close()
        return True
    return False


if __name__ == '__main__':
    if mysql_write_hash(db_config['RECORDS']):
        logging.info("Hashes successfully written to the database.")
    else:
        logging.error("Failed to write hashes to the database.")
