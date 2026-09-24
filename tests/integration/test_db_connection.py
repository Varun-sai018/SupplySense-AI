import os
import sys

# Add the project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from common.database import get_connection

def test_db_connection():
    """
    Test utility to verify the database connection using the centralized module.
    """
    print("Testing MySQL connection...")
    connection = None
    try:
        connection = get_connection()
        cursor = connection.cursor()
        cursor.execute("SELECT 1 AS success")
        result = cursor.fetchone()
        
        if result and result.get('success') == 1:
            print("SUCCESS: Connected to MySQL database.")
        else:
            print("FAILURE: Connection succeeded but query failed.")
    except Exception as e:
        print(f"FAILURE: Could not connect to database. Error: {e}")
    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    test_db_connection()
