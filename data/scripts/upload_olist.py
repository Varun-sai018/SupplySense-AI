import os
import sys
import pandas as pd
import logging

# Ensure repository root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from common.database import get_connection
from config import settings

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# ============================================================
# CSV FILES -> MYSQL TABLES
# ============================================================

FILES = {
    "olist_orders": "olist_orders_dataset.csv",
    "olist_order_items": "olist_order_items_dataset.csv",
    "olist_products": "olist_products_dataset.csv",
    "olist_sellers": "olist_sellers_dataset.csv",
    "olist_customers": "olist_customers_dataset.csv",
    "olist_order_payments": "olist_order_payments_dataset.csv",
    "olist_order_reviews": "olist_order_reviews_dataset.csv",
    "olist_geolocation": "olist_geolocation_dataset.csv",
    "product_category_name_translation": "product_category_name_translation.csv"
}

DATETIME_COLUMNS = {
    "olist_orders": [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ],
    "olist_order_items": [
        "shipping_limit_date"
    ],
    "olist_order_reviews": [
        "review_creation_date",
        "review_answer_timestamp"
    ]
}


def clean_columns(df):
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
    )
    return df


def convert_datetime(df, table_name):
    if table_name not in DATETIME_COLUMNS:
        return df

    for column in DATETIME_COLUMNS[table_name]:
        if column in df.columns:
            logger.info(f"   Converting '{column}' to datetime...")
            df[column] = pd.to_datetime(df[column], errors="coerce")
    return df


def convert_value(value):
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if hasattr(value, "item"):
        try:
            return value.item()
        except:
            pass
    return value


def create_table(cursor, table_name, df):
    logger.info("   Removing old table if it exists...")
    cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")

    column_definitions = []
    for column in df.columns:
        dtype = df[column].dtype
        if pd.api.types.is_integer_dtype(dtype):
            mysql_type = "BIGINT"
        elif pd.api.types.is_float_dtype(dtype):
            mysql_type = "DOUBLE"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            mysql_type = "DATETIME"
        else:
            mysql_type = "TEXT"
        
        column_definitions.append(f"`{column}` {mysql_type}")

    create_sql = f"""
        CREATE TABLE `{table_name}` (
            {", ".join(column_definitions)}
        )
        ENGINE=InnoDB
        DEFAULT CHARSET=utf8mb4
    """
    cursor.execute(create_sql)
    logger.info(f"   Table '{table_name}' created.")


def insert_data(connection, cursor, table_name, df):
    columns = list(df.columns)
    column_names = ", ".join(f"`{column}`" for column in columns)
    placeholders = ", ".join(["%s"] * len(columns))

    insert_sql = f"""
        INSERT INTO `{table_name}`
        ({column_names})
        VALUES ({placeholders})
    """

    records = []
    for row in df.itertuples(index=False, name=None):
        converted_row = tuple(convert_value(value) for value in row)
        records.append(converted_row)

    chunk_size = 2000
    total_rows = len(records)

    for start in range(0, total_rows, chunk_size):
        end = min(start + chunk_size, total_rows)
        chunk = records[start:end]
        cursor.executemany(insert_sql, chunk)
        connection.commit()
        logger.info(f"   Inserted {end:,} / {total_rows:,} rows")


def upload_file(table_name, file_name):
    file_path = os.path.join(settings.OLIST_DATA_DIR, file_name)

    print("\n" + "=" * 70)
    print(f"FILE  : {file_name}")
    print(f"TABLE : {table_name}")
    print("=" * 70)

    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return False

    connection = None
    try:
        logger.info("1. Reading CSV...")
        df = pd.read_csv(file_path, low_memory=False, encoding="utf-8")
        
        logger.info(f"   Rows    : {len(df):,}")
        logger.info(f"   Columns : {len(df.columns)}")
        
        logger.info("2. Cleaning column names...")
        df = clean_columns(df)
        
        logger.info("3. Processing datetime columns...")
        df = convert_datetime(df, table_name)
        
        logger.info("4. Connecting to database...")
        connection = get_connection()
        cursor = connection.cursor()
        
        logger.info("5. Creating MySQL table...")
        create_table(cursor, table_name, df)
        
        logger.info("6. Uploading data...")
        insert_data(connection, cursor, table_name, df)
        
        logger.info("7. Verifying data...")
        cursor.execute(f"SELECT COUNT(*) as count FROM `{table_name}`")
        mysql_count_row = cursor.fetchone()
        mysql_count = mysql_count_row['count'] if isinstance(mysql_count_row, dict) else mysql_count_row[0]

        logger.info(f"   CSV rows   : {len(df):,}")
        logger.info(f"   MySQL rows : {mysql_count:,}")

        if mysql_count == len(df):
            logger.info(f"SUCCESS: '{table_name}' imported correctly.")
            return True
        else:
            logger.warning(f"WARNING: Row count mismatch for '{table_name}'.")
            return False

    except UnicodeDecodeError:
        logger.warning("UTF-8 failed. Trying latin-1 encoding...")
        try:
            if connection:
                connection.close()

            df = pd.read_csv(file_path, low_memory=False, encoding="latin-1")
            df = clean_columns(df)
            df = convert_datetime(df, table_name)

            connection = get_connection()
            cursor = connection.cursor()

            create_table(cursor, table_name, df)
            insert_data(connection, cursor, table_name, df)

            logger.info(f"SUCCESS: '{table_name}' imported using latin-1.")
            return True

        except Exception as e:
            logger.error(f"ERROR uploading '{table_name}': {e}")
            return False

    except Exception as e:
        logger.error(f"ERROR uploading '{table_name}': {e}")
        if connection:
            try:
                connection.rollback()
            except:
                pass
        return False
    finally:
        if connection:
            connection.close()


def main():
    print("\n" + "=" * 70)
    print("        SUPPLYSENSE AI - OLIST DATA IMPORT")
    print("=" * 70)

    successful = []
    failed = []

    for table_name, file_name in FILES.items():
        result = upload_file(table_name, file_name)
        if result:
            successful.append(table_name)
        else:
            failed.append(table_name)

    print("\n\n" + "=" * 70)
    print("                    IMPORT SUMMARY")
    print("=" * 70)

    print("\nSuccessfully imported:")
    if successful:
        for table in successful:
            print(f"   [OK] {table}")
    else:
        print("   None")

    print("\nFailed imports:")
    if failed:
        for table in failed:
            print(f"   [FAILED] {table}")
    else:
        print("   None")

    print("\n" + "=" * 70)
    print("Tables currently available in supplysense:")
    print("=" * 70)

    connection = None
    try:
        connection = get_connection()
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        for table in tables:
            # Handle dict or tuple format
            t_name = list(table.values())[0] if isinstance(table, dict) else table[0]
            print(f"   - {t_name}")
    except Exception as e:
        logger.error(f"Could not retrieve table list: {e}")
    finally:
        if connection:
            connection.close()

    print("\n" + "=" * 70)
    print("        IMPORT PROCESS COMPLETED")
    print("=" * 70)


if __name__ == "__main__":
    main()
