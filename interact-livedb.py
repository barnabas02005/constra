import os
import time
import requests
import threading
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import math
import schedule
import traceback

from db_config import Database
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# Fetch environment variables
host = os.getenv("DB_HOST")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
database = os.getenv("DB_NAME")
port = int(os.getenv("DB_PORT"))
API_URL = os.getenv("SAVE_TRADE_API")
UPDATE_API_URL = os.getenv("UPDATE_TRADE_API")
TOKEN = os.getenv("BACKUP_API_TOKEN")

db_conn = Database(
    host=host,
    user=user,
    password=password,
    database=database,
    port=port
)

def add_columns(table_name, column_definitions):
    """
    Adds one or more columns to a table with optional positioning.
    
    Args:
        table_name (str): The name of the table to alter.
        column_definitions (list of str): Column definitions, e.g. 
            ["email VARCHAR(100) AFTER name", "status TINYINT(1) AFTER email"]
    
    Returns:
        bool: True if successful, False otherwise.
    """
    conn = db_conn.get_connection()
    try:
        with conn.cursor() as cursor:
            alter_parts = [f"ADD COLUMN {definition}" for definition in column_definitions]
            alter_sql = f"ALTER TABLE `{table_name}` " + ", ".join(alter_parts) + ";"
            cursor.execute(alter_sql)
        
        conn.commit()
        print(f"✅ Columns added to `{table_name}`: {column_definitions}")
        return True

    except Exception as e:
        print(f"❌ Failed to add columns to `{table_name}`: {e}")
        return False
    
def run_schema_setup():
    sql_statements = [
        # #Drop triggers if they exist
        # "DROP TRIGGER IF EXISTS opn_trade_after_insert;",
        # "DROP TRIGGER IF EXISTS opn_trade_after_update;",
        # "DROP TRIGGER IF EXISTS opn_trade_after_delete;",

        # Create table
        """
        CREATE TABLE IF NOT EXISTS redis_notify (
            id INT AUTO_INCREMENT PRIMARY KEY,
            action_type VARCHAR(10),
            table_name VARCHAR(64),
            row_id INT,
            user_cred_id VARCHAR(64),
            published TINYINT DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        # INSERT trigger
        """
        DROP TRIGGER IF EXISTS opn_trade_after_insert;
        """,
        """
        CREATE TRIGGER opn_trade_after_insert
        AFTER INSERT ON opn_trade
        FOR EACH ROW
        BEGIN
            INSERT INTO redis_notify (action_type, table_name, row_id, user_cred_id)
            VALUES ('insert', 'opn_trade', NEW.id, NEW.user_cred_id);
        END;
        """,
        # UPDATE trigger
        """
        DROP TRIGGER IF EXISTS opn_trade_after_update;
        """,
        """
        CREATE TRIGGER opn_trade_after_update
        AFTER UPDATE ON opn_trade
        FOR EACH ROW
        BEGIN
            INSERT INTO redis_notify (action_type, table_name, row_id, user_cred_id)
            VALUES ('update', 'opn_trade', NEW.id, NEW.user_cred_id);
        END;
        """,
        # DELETE trigger
        """
        DROP TRIGGER IF EXISTS opn_trade_after_delete;
        """,
        """
        CREATE TRIGGER opn_trade_after_delete
        AFTER DELETE ON opn_trade
        FOR EACH ROW
        BEGIN
            INSERT INTO redis_notify (action_type, table_name, row_id, user_cred_id)
            VALUES ('delete', 'opn_trade', OLD.id, OLD.user_cred_id);
        END;
        """
    ]


    conn = db_conn.get_connection()
    cursor = conn.cursor()
    for stmt in sql_statements:
        try:
            cursor.execute(stmt)
            print("✅ Executed:", stmt.strip().split("\n")[0][:60])
        except Exception as err:
            print("⚠️ Error executing SQL:", err)
    conn.commit()
    cursor.close()
    conn.close()
    
    
if __name__ == "__main__":
    run_schema_setup()
      
# addColumn = add_columns(
#                 "opn_trade",
#                 [
#                     "allow_rentry INT(20) AFTER amount",
#                     "re_entry_count INT(11) AFTER trade_done"
#                 ]
#             )
# if addColumn:
#   print("Columns added.")