import csv
import logging
import os
import sys
from datetime import datetime

import psycopg2
from psycopg2 import sql

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("csv_import.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

DB_CONFIG = {
    'host': 'localhost',
    'database': 'test_db',
    'user': 'postgres',
    'password': 'your_password'
}

def create_table_if_not_exists(conn):
    create_query = """
    CREATE TABLE IF NOT EXISTS sales_data (
        id SERIAL PRIMARY KEY,
        product_name VARCHAR(255),
        quantity INTEGER,
        price DECIMAL(10, 2),
        sale_date DATE
    );
    """
    with conn.cursor() as cur:
        cur.execute(create_query)
        conn.commit()
    logger.info("Таблица sales_data готова.")

def validate_row(row):
    try:
        int(row['quantity'])
        float(row['price'])
        datetime.strptime(row['sale_date'], '%Y-%m-%d')
        return True
    except (ValueError, KeyError):
        return False

def import_csv_to_db(csv_path, conn):
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        data_to_insert = []

        for row_num, row in enumerate(reader, start=2):
            if not validate_row(row):
                logger.warning(f"Пропущена некорректная строка {row_num}: {row}")
                continue
            data_to_insert.append((
                row['product_name'],
                int(row['quantity']),
                float(row['price']),
                row['sale_date']
            ))

        if not data_to_insert:
            logger.error("Нет валидных данных для импорта.")
            return

        insert_query = sql.SQL("""
            INSERT INTO sales_data (product_name, quantity, price, sale_date)
            VALUES (%s, %s, %s, %s)
        """)

        with conn.cursor() as cur:
            cur.executemany(insert_query, data_to_insert)
            conn.commit()

        logger.info(f"Успешно импортировано {len(data_to_insert)} записей из {csv_path}.")

if __name__ == "__main__":
    csv_file = "sample_data.csv"

    if not os.path.exists(csv_file):
        logger.error(f"Файл {csv_file} не найден!")
        sys.exit(1)

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        create_table_if_not_exists(conn)
        import_csv_to_db(csv_file, conn)
    except Exception as e:
        logger.error(f"Ошибка подключения к БД: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
