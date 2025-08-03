def ensure_exchanges_table_exists(db_conn):
  create_table_sql = """
  CREATE TABLE IF NOT EXISTS exchanges (
      id INT AUTO_INCREMENT PRIMARY KEY,
      exchange_name VARCHAR(255) NOT NULL,
      requirePass INT NOT NULL DEFAULT 0,
      status INT NOT NULL DEFAULT 1,
      date_added DATETIME DEFAULT CURRENT_TIMESTAMP
  );
  """
  try:
    conn = db_conn.get_connection()
    with conn.cursor() as cursor:
        cursor.execute(create_table_sql)
    conn.commit()
  finally:
    conn.close()

def ensure_trade_signal_exists(db_conn):
  create_table_sql = """
  CREATE TABLE IF NOT EXISTS trade_signal (
      id INT AUTO_INCREMENT PRIMARY KEY,
      exchange INT NOT NULL,
      symbol_pair VARCHAR(255) NOT NULL,
      trade_type INT NOT NULL,
      status INT NOT NULL DEFAULT 1,
      date_added DATETIME DEFAULT CURRENT_TIMESTAMP,
      UNIQUE KEY unique_exchange_symbol (symbol_pair)
  );
  """
  try:
    conn = db_conn.get_connection()
    with conn.cursor() as cursor:
        cursor.execute(create_table_sql)
    conn.commit()
  finally:
    conn.close()
    
def insert_trade_signal(db_conn, data):
  try:
    ensure_trade_signal_exists(db_conn)
    conn = db_conn.get_connection()
    with conn.cursor() as cursor:
      sql = """
          INSERT INTO trade_signal (exchange, symbol_pair, trade_type, status)
          VALUES (%s, %s, %s, %s)
          ON DUPLICATE KEY UPDATE status = VALUES(status)
      """
      cursor.execute(sql, (
          data['exchange'],
          data['symbol_pair'],
          data['trade_type'],
          data['status']
      ))
    conn.commit()
    print("✅ Insert successful")
    return True
  except Exception as e:
    print("❌ Insert failed:", e)
    return False
  finally:
    try:
        conn.close()
    except:
        pass  # ignore close error if conn was never set

def ensure_user_cred_table_exists(db_conn):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS user_cred (
        id INT AUTO_INCREMENT PRIMARY KEY,
        user_id INT NOT NULL,
        exchange_id INT NOT NULL,
        api_key VARCHAR(255) NOT NULL,
        secret VARCHAR(255) NOT NULL,
        password VARCHAR(200) DEFAULT NULL,
        status INT NOT NULL DEFAULT 1,
        date_added DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    """
    conn = db_conn.get_connection()
    if not conn:
        print("❌ No DB connection")
        return []
    with conn.cursor() as cursor:
        cursor.execute(create_table_sql)
    conn.commit()
    
def ensure_hedge_limit_table_exists(db_conn):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS hedge_limit (
        id INT AUTO_INCREMENT PRIMARY KEY,
        user_id INT NOT NULL,
        trade_id INT NOT NULL,
        order_id VARCHAR(255) NOT NULL,
        trade_type INT NOT NULL,
        status INT NOT NULL DEFAULT 1,
        date_added DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    """
    conn = db_conn.get_connection()
    if not conn:
        print("❌ No DB connection")
        return []
    with conn.cursor() as cursor:
        cursor.execute(create_table_sql)
    conn.commit()
    
def ensure_opn_trade_table_exists(db_conn):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS opn_trade (
        id INT AUTO_INCREMENT PRIMARY KEY,
        user_cred_id INT NOT NULL,
        trade_signal INT NOT NULL,
        order_id VARCHAR(255) NOT NULL,
        symbol VARCHAR(255) NOT NULL,
        trade_type INT NOT NULL,
        amount DOUBLE(16, 3) DEFAULT 0.000,
        lv_size DOUBLE(16, 3) DEFAULT 0.000,
        allow_rentry INT(20),
        leverage INT NOT NULL,
        trail_order_id VARCHAR(255),
        trail_threshold DOUBLE(16, 4) DEFAULT 0.0000,
        profit_target_distance DOUBLE(16, 4) DEFAULT 0.0000,
        to_liquidation_order_id VARCHAR(255),
        trade_done TINYINT DEFAULT 0,
        re_entry_count INT(11),
        dn_allow_rentry INT(11) NOT NULL DEFAULT 0,
        status TINYINT NOT NULL DEFAULT 1,
        date_added DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    """
    try:
        ensure_hedge_limit_table_exists(db_conn)
        conn = db_conn.get_connection()
        with conn.cursor() as cursor:
            cursor.execute(create_table_sql)
        conn.commit()
    except Exception as e:
        print(f"❌ Failed to create table: {e}")
    finally:
        conn.close()

def get_all_credentials_with_exchange_info(db_conn):
    ensure_user_cred_table_exists(db_conn)
    ensure_opn_trade_table_exists(db_conn)
    conn = db_conn.get_connection()
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT uc.id AS cred_id,
                   uc.api_key,
                   uc.secret,
                   uc.password,
                   uc.exchange_id,
                   ex.exchange_name,
                   ex.requirePass
            FROM user_cred uc
            JOIN exchanges ex ON uc.exchange_id = ex.id
            WHERE uc.status = 1
        """)
        return cursor.fetchall()
      
def get_exchange_by_id(db_conn, id):
  ensure_exchanges_table_exists()
  conn = db_conn.get_connection()
  with conn.cursor() as cursor:
      cursor.execute(f"SELECT * FROM exchanges WHERE id = {id} AND status = 1")
      return cursor.fetchall()
  
def fetch_single_trade_signal(db_conn):
    conn = db_conn.get_connection()
    if conn is None:
        print("❌ DB connection is None")
        return None

    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT * FROM trade_signal 
                WHERE status = 1 
                ORDER BY RAND() + UNIX_TIMESTAMP(date_added) / 1000000 DESC 
                LIMIT 1
            """)
            return cursor.fetchone()
    except Exception as e:
        print(f"❌ Error fetching trade signal: {e}")
        return None
    finally:
        conn.close()

def has_open_trade(db_conn, user_cred_id, symbol):
    try:
        conn = db_conn.get_connection()
        with conn.cursor() as cursor:
            query = """
                SELECT 1 FROM opn_trade
                WHERE user_cred_id = %s AND symbol = %s AND status = 1
                LIMIT 1
            """
            cursor.execute(query, (user_cred_id, symbol))
            result = cursor.fetchone()
            return result is not None
    except Exception as e:
        print(f"Error checking trade: {e}")
        return False

def get_side_count(db_conn, user_cred_id, trade_done, side):
    try:
        conn = db_conn.get_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) AS count FROM opn_trade
                WHERE user_cred_id = %s AND trade_type = %s AND trade_done = %s AND status = 1
            """, (user_cred_id, side, trade_done))
            result = cursor.fetchone()
            return result['count']
    except Exception as e:
        print(f"Error Fetching trade side count for User Cred ID: {user_cred_id} \nError: {e}")
        return False
 
# 🔍 Fetch full row from DB
def fetch_row(db_conn, table, row_id):
    conn = db_conn.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table} WHERE id = %s", (row_id,))
    return cursor.fetchone()

# 🔍 Fetch full limit hedge row from DB table
def fetch_row_details(db_conn, table, user_id, trade_id, trade_type):
    conn = db_conn.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table} WHERE user_id = %s AND trade_id = %s AND trade_type = %s AND status = 1", (user_id, trade_id, trade_type))
    return cursor.fetchone()

# 🔍 Fetch full row from DB
def GetParentTrade(db_conn, table, user_id, symbol, trade_type):
    conn = db_conn.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"SELECT id FROM {table} WHERE user_cred_id = %s AND symbol = %s AND trade_type = %s", (user_id, symbol, trade_type))
    result = cursor.fetchone()
    return result['id'] if result else 0

def GetHedgeSummationProfit(db_conn, trade_id):
    conn = db_conn.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"SELECT SUM(realizedPnL) AS hedge_total_profit FROM opn_trade WHERE child_to = %s", (trade_id))
    result = cursor.fetchone()
    # print("summhedgeproift: ", result)
    if result is None or result.get('hedge_total_profit') is None:
        return 0.0
    return float(result['hedge_total_profit'])

def has_limit_hedge_open(db_conn, table, user_id, trade_id, hedge_side_int):
    conn = db_conn.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table} WHERE user_id = %s AND trade_id = %s AND trade_type = %s", (user_id, trade_id, hedge_side_int))
    result = cursor.fetchone()
    # print(f"Result(Hedge_limit): {result}")
    return True if result else False

def insert_trade_data(db_conn, data):
    conn = db_conn.get_connection()
    try:
        with conn.cursor() as cursor:
            sql = """
                INSERT INTO opn_trade (strategy_type, user_cred_id, child_to, trade_signal, order_id, symbol, trade_type, amount, leverage, trail_threshold, profit_target_distance, cum_close_threshold, cum_close_distance, trade_done, re_entry_count, hedged, hedge_start, status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql, (
                data['strategy_type'],
                data['user_cred_id'],
                data['child_to'],
                data['trade_signal'],
                data['order_id'],
                data['symbol'],
                data['trade_type'],
                data['amount'],
                data['leverage'],
                data['trail_threshold'],
                data['profit_target_distance'],
                data['cum_close_threshold'],
                data['cum_close_distance'],
                data['trade_done'],
                data['re_entry_count'],
                data['hedged'],
                data['hedge_start'],
                data['status'],
            ))
            conn.commit()
            return True
            print(f"✅ Insert {data['symbol']} successful: (Take<-->Trade)")
    except Exception as e:
        print("❌ Insert failed:", e)
        return False
    finally:
        try:
            conn.close()
        except:
            pass  # ignore close error if conn was never set
        
def insert_hedge_limit_data(db_conn, data):
    conn = db_conn.get_connection()
    try:
        with conn.cursor() as cursor:
            sql = """
                INSERT INTO hedge_limit (user_id, trade_id, order_id, trade_type, status) VALUES (%s, %s, %s, %s, 1)
            """
            cursor.execute(sql, (
                data['user_id'],
                data['trade_id'],
                data['order_id'],
                data['trade_type'],
            ))
            conn.commit()
            return True
            print(f"✅ Insert {data['order_id']} successful: (Limit Hedge set<-->Trade)")
    except Exception as e:
        print("❌ Insert failed:", e)
        return False
    finally:
        try:
            conn.close()
        except:
            pass  # ignore close error if conn was never set

def update_row(db_conn, table_name, updates, conditions):
    """
    Updates rows in any table with flexible WHERE conditions and operators.

    :param table_name: Table name to update
    :param updates: Dict of column-value pairs to set
    :param conditions: Dict with values OR (operator, value) tuples for WHERE clause
    :return: True if any rows updated
    """
    if not updates or not conditions:
        raise ValueError("Both updates and conditions must be provided.")

    conn = db_conn.get_connection()
    with conn.cursor() as cursor:
        # SET clause
        set_clause = ', '.join([f"`{key}` = %s" for key in updates])
        update_values = list(updates.values())

        # WHERE clause with operator handling
        where_clauses = []
        where_values = []
        for key, val in conditions.items():
            if isinstance(val, tuple) and len(val) == 2:
                op, v = val
                where_clauses.append(f"`{key}` {op} %s")
                where_values.append(v)
            else:
                where_clauses.append(f"`{key}` = %s")
                where_values.append(val)

        where_clause = ' AND '.join(where_clauses)

        query = f"UPDATE `{table_name}` SET {set_clause} WHERE {where_clause}"
        cursor.execute(query, update_values + where_values)
        conn.commit()
        return cursor.rowcount > 0


def delete_row(db_conn, table_name, conditions, log_table=None):
    """
    Deletes rows from any table with advanced WHERE conditions.
    Optionally logs the deleted rows to another table.

    :param table_name: Name of the table to delete from
    :param conditions: Dict of column: value, or column: (op, value), or column: ('IN', [list])
    :param log_table: Optional name of a table to insert deleted rows into before deletion
    :return: Number of rows deleted
    """
    if not conditions:
        raise ValueError("Conditions must be provided for safety.")

    conn = db_conn.get_connection()
    with conn.cursor() as cursor:
        # Build WHERE clause
        where_clauses = []
        where_values = []

        for col, val in conditions.items():
            if isinstance(val, tuple) and len(val) == 2:
                op, v = val
                if op.upper() == 'IN' and isinstance(v, list):
                    placeholders = ', '.join(['%s'] * len(v))
                    where_clauses.append(f"`{col}` IN ({placeholders})")
                    where_values.extend(v)
                else:
                    where_clauses.append(f"`{col}` {op} %s")
                    where_values.append(v)
            else:
                where_clauses.append(f"`{col}` = %s")
                where_values.append(val)

        where_clause = ' AND '.join(where_clauses)

        # Optional: Backup rows to log table
        if log_table:
            log_query = f"""
                INSERT INTO `{log_table}` SELECT * FROM `{table_name}` WHERE {where_clause}
            """
            cursor.execute(log_query, where_values)

        # Delete rows
        delete_query = f"DELETE FROM `{table_name}` WHERE {where_clause}"
        cursor.execute(delete_query, where_values)

        conn.commit()
        return cursor.rowcount > 0
    
def add_columns(db_conn, table_name, column_definitions):
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
      
def truncate_table(db_conn, table_name):
    conn = db_conn.get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE `{table_name}`")
        conn.commit()
        print(f"✅ {table_name} table truncated (all rows cleared).")
        return True
    except Exception as e:
        print(f"❌ Failed to truncate table: {e}")
        return False
          
def drop_table(db_conn, table_name):
  conn = db_conn.get_connection()
  try:
      with conn.cursor() as cursor:
          cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
      conn.commit()
      print(f"✅ {table_name} table deleted.")
  except Exception as e:
      print(f"❌ Failed to drop table: {e}")
      
def clear_trade_signals_for_exchange(db_conn, exchange_db_id):
  try:
      with api_db_conn.cursor() as cursor:
          cursor.execute("DELETE FROM trade_signal WHERE exchange = %s", (exchange_db_id,))
      api_db_conn.commit()
      print(f"✅ Cleared trade signals for exchange ID {exchange_db_id}")
  except Exception as e:
      print(f"❌ Failed to clear trade signals: {e}")


__all__ = [name for name in globals() if not name.startswith("_")]