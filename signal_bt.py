import os
import threading
import time
from datetime import datetime
import traceback
import math
import json
import schedule
from concurrent.futures import ThreadPoolExecutor
import ccxt
import pandas as pd
import ta

from db_config import Database
from dotenv import load_dotenv
from utils.db_func import *


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

# GLOBAL VARIABLES
timeframe = '1h'
limit = 10
print_lock = threading.Lock()

def thread_safe_print(*args, **kwargs):
    with print_lock:
        print(*args, **kwargs)

def count_sig_digits(precision):
    # Count digits after decimal point if it's a fraction
    if precision < 1:
        return abs(int(round(math.log10(precision))))
    else:
        return 1  # Treat whole numbers like 1, 10, 100 as 1 sig digit

def round_to_sig_figs(num, sig_figs):
    if num == 0:
        return 0
    return round(num, sig_figs - int(math.floor(math.log10(abs(num)))) - 1)

def get_exchanges():
    try:
        ensure_exchanges_table_exists(db_conn)
        ensure_trade_signal_exists(db_conn)
        conn = db_conn.get_connection()
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM exchanges WHERE status = 1")
            return cursor.fetchall()
    except Exception as e:
        print("❌ Getting Exchanges failed:", e)

      
def clear_trade_signals_for_exchange(exchange_db_id):
  try:
      with api_db_conn.cursor() as cursor:
          cursor.execute("DELETE FROM trade_signal WHERE exchange = %s", (exchange_db_id,))
      api_db_conn.commit()
      print(f"✅ Cleared trade signals for exchange ID {exchange_db_id}")
  except Exception as e:
      print(f"❌ Failed to clear trade signals: {e}")


def create_exchange(exchange_name):
    return getattr(ccxt, exchange_name)({
        'enableRateLimit': True
    })

stop_event = threading.Event()

# ---------- MARKET CACHE -----------
market_cache = {}
market_cache_lock = threading.Lock()

def load_markets_once_per_hour(exchange):
    now = time.time()
    with market_cache_lock:
        cached = market_cache.get(exchange.id)
        if cached and (now - cached['last_fetch'] < 3600):
            return cached['markets'], cached['filtered_symbols']

    try:
        clear_table = truncate_table(db_conn, "trade_signal")
        if clear_table:
            markets = exchange.load_markets()
            filtered_symbols = [
                symbol for symbol, market in markets.items()
                if ":USDT" in symbol and market.get('active', True) and market.get('info', {}).get('type') == "PerpetualV2"
            ]

            with market_cache_lock:
                market_cache[exchange.id] = {
                    'markets': markets,
                    'filtered_symbols': filtered_symbols,
                    'last_fetch': now
                }
            return markets, filtered_symbols

    except Exception as e:
        thread_safe_print(f"❌ Failed to load markets for {exchange.id}: {e}")
        return None, []

# ---------- OHLCV CACHE WITH TIMEFRAME-BASED EXPIRY -----------

ohlcv_cache = {}
ohlcv_cache_lock = threading.Lock()

def timeframe_to_seconds(timeframe: str) -> int:
    if timeframe.endswith('m'):
        return int(timeframe[:-1]) * 60
    elif timeframe.endswith('h'):
        return int(timeframe[:-1]) * 3600
    elif timeframe.endswith('d'):
        return int(timeframe[:-1]) * 86400
    else:
        # Default fallback 1 hour
        return 3600

def get_ohlcv_cached(exchange, symbol, timeframe='1h', limit=10):
    now = time.time()
    expire_seconds = timeframe_to_seconds(timeframe)

    key = (exchange.id, symbol, timeframe)
    with ohlcv_cache_lock:
        cached = ohlcv_cache.get(key)
        if cached and (now - cached['timestamp'] < expire_seconds):
            return cached['data']

    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        with ohlcv_cache_lock:
            ohlcv_cache[key] = {
                'data': ohlcv,
                'timestamp': now
            }
        return ohlcv
    except ccxt.errors.RateLimitExceeded as e:
        thread_safe_print(f"Rate limit hit on {exchange.id} for {symbol}, backing off: {e}")
        time.sleep(5)
        return None
    except Exception as e:
        thread_safe_print(f"Error fetching OHLCV for {symbol} on {exchange.id}: {e}")
        return None

# ---------- TRADE SIGNAL CHECK -----------

def check_trade_signal(exchange, symbol, ohlcv):
    if not ohlcv or len(ohlcv) < 10:
        return False, None, {}

    df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

    df['EMA_9'] = df['close'].ewm(span=9, adjust=False).mean()
    df['EMA_21'] = df['close'].ewm(span=21, adjust=False).mean()

    atr = ta.volatility.AverageTrueRange(df['high'], df['low'], df['close'], window=10).average_true_range()
    rsi = ta.momentum.RSIIndicator(df['close'], window=10).rsi()

    df['atr'] = atr
    df['rsi'] = rsi

    latest_close = df['close'].iloc[-1]
    latest_atr = df['atr'].iloc[-1]
    normalized_atr = latest_atr / latest_close
    ema_9 = df['EMA_9'].iloc[-1]
    ema_21 = df['EMA_21'].iloc[-1]
    rsi_now = df['rsi'].iloc[-1]
    rsi_prev = df['rsi'].iloc[-2]

    trend = "uptrend" if ema_9 > ema_21 else "downtrend" if ema_9 < ema_21 else "sideways"

    should_trade, side = False, None
    if normalized_atr > 0.015:
        if trend == "downtrend" and rsi_now <= 30:
            should_trade, side = True, 'buy'
        elif trend == "uptrend" and rsi_now >= 70:
            should_trade, side = True, 'sell'

    return should_trade, side, {
        'atr': latest_atr,
        'atr_norm': normalized_atr,
        'ema_9': ema_9,
        'ema_21': ema_21,
        'rsi_now': rsi_now,
        'rsi_prev': rsi_prev,
        'trend': trend
    }

# ---------- MAIN JOB AND SCHEDULER -----------
def main_job(exchange, exchange_db_id, timeframe='1h'):

    # ---- Your job logic starts here ----
    markets, all_symbols = load_markets_once_per_hour(exchange)
    if not markets or not all_symbols:
        thread_safe_print(f"⚠️ No markets or symbols data for {exchange.id}, skipping.")
        return

    remaining_symbols = set(all_symbols) # mutable list to track progress
    remaining_symbols_lock = threading.Lock()  
    retry_delay = 10  # seconds to wait on rate limit
    max_retries = 3   # optional: max attempts

    attempts = 0
    while remaining_symbols and attempts < max_retries:
        thread_safe_print(f"🔁 Processing {len(remaining_symbols)} symbols for {exchange.id}, Attempt {attempts + 1}")

        failed_due_to_rate_limit = []

        def process_symbol(symbol):
            nonlocal failed_due_to_rate_limit
            if stop_event.is_set():
              return  # 🚫 Stop immediately if shutdown is requested
            try:
                ohlcv = get_ohlcv_cached(exchange, symbol, timeframe=timeframe)
                if ohlcv is None or stop_event.is_set():
                  return  # extra early-exit check
                signal, side, details = check_trade_signal(exchange, symbol, ohlcv)
                # thread_safe_print(f"{symbol} → Signal: {signal}, Side: {side}, Trend: {details.get('trend', 'N/A')} :: {exchange.id}")
                
                
                if signal:
                    if side == 'buy':
                        return
                    
                    side_int = 0 if side == 'buy' else 1 if side == 'sell' else None
                    thread_safe_print(f"🚨 [{symbol}] Trade {side} signal detected.")
                    trade_signal_data = {
                        "exchange": exchange_db_id,
                        "symbol_pair": symbol,
                        "trade_type": side_int,
                        "status": 1
                    }
                  
                    if insert_trade_signal(db_conn, trade_signal_data):
                        with remaining_symbols_lock:
                            remaining_symbols.discard(symbol)  # ✅ remove from retry list
                   
                    # drop_table("trade_signal")
                    # Add your trade execution or logging logic here

            except ccxt.errors.RateLimitExceeded as e:
                thread_safe_print(f"⏳ Rate limit hit on {exchange.id} for {symbol}, will retry after delay.")
                failed_due_to_rate_limit.append(symbol)
            except Exception as e:
                thread_safe_print(f"❌ Error in {symbol}: {e}")
                traceback.print_exc()

        with ThreadPoolExecutor(max_workers=5) as executor:
            executor.map(process_symbol, remaining_symbols)

        if failed_due_to_rate_limit:
            thread_safe_print(f"⏸️ Pausing {retry_delay}s due to rate limit before retrying {len(failed_due_to_rate_limit)} symbols...")
            time.sleep(retry_delay)
            with remaining_symbols_lock:
              remaining_symbols = set(failed_due_to_rate_limit)  # retry only failed ones
            attempts += 1
        else:
            break  # all good

    if remaining_symbols:
        thread_safe_print(f"⚠️ Gave up retrying {len(remaining_symbols)} symbols for {exchange.id} after {max_retries} attempts.")


def run_exchange_scheduler(exchange, exchange_db_id, timeframe='1h', cooldown_seconds=180, interval_seconds=1800):
    while not stop_event.is_set():
        try:
            thread_safe_print(f"🔁 Running scheduler loop for {exchange.id} at {datetime.now().strftime('%H:%M:%S')}")
            main_job(exchange=exchange, exchange_db_id=exchange_db_id, timeframe=timeframe)
            time.sleep(interval_seconds)
        except Exception:
            thread_safe_print(f"⚠️ Scheduler error for {exchange.id}. Retrying in 10 seconds...")
            traceback.print_exc()
            time.sleep(10)

def run_all():
    all_exchanges = get_exchanges()
    if not all_exchanges:
        thread_safe_print("⚠️ No exchanges found in DB. Exiting.")
        return

    exchange_list = []
    for exch_data in all_exchanges:
        try:
            exchange = create_exchange(exch_data['exchange_name'].lower())
            thread_safe_print(f"Instance created for exchange: {exchange.id}")
            exchange_list.append({
                'exchange': exchange,
                'exchange_name': exch_data['exchange_name'],
                'exchange_id': exch_data['id']
            })
        except Exception as e:
            thread_safe_print(f"❌ Failed to create exchange instance for {exch_data['exchange_name']}: {e}")

    if not exchange_list:
        thread_safe_print("⚠️ No valid exchanges to run. Exiting.")
        return

    threads = []
    for exch in exchange_list:
        t = threading.Thread(
            target=run_exchange_scheduler,
            args=(exch['exchange'], exch['exchange_id']),
            daemon=True  # THIS allows Ctrl+C to immediately stop the program
        )
        t.start()
        threads.append(t)

    try:
        while not stop_event.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        thread_safe_print("\n🛑 Ctrl+C detected, stopping all threads...")
        stop_event.set()
        # for t in threads:
        #     t.join(timeout=10)
        # thread_safe_print("✅ All threads stopped.")

if __name__ == "__main__":
    run_all()