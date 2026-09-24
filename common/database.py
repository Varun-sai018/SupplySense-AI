import pymysql
import pymysql.cursors
from config import settings

def get_connection():
    """
    Creates and returns a PyMySQL connection using the centralized configuration.
    
    Returns:
        pymysql.connections.Connection: An open database connection.
    """
    return pymysql.connect(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
        database=settings.DB_NAME,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor
    )
