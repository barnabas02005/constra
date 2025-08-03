import os
import sys
import io
import time
import requests
import datetime
import json
from collections import defaultdict
# import redis.asyncio as redis
import asyncio
import threading
from threading import Lock
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import math
import schedule
import traceback
import ccxt

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')


from db_config import Database
from dotenv import load_dotenv
from utils.db_func import *
from utils.trade_exec import *


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

stop_event = threading.Event()  # Global stop signal
print_lock = threading.Lock()
buffer_lock = threading.Lock()
symbol_buffers = defaultdict(list)
# Global lock dictionary for symbols
re_symbol_locks = {}
re_symbol_locks_lock = threading.Lock()  # Protects symbol_locks
# Global symbol-to-thread tracker per exchange key
symbol_locks = {}
symbol_locks_lock = threading.Lock()
# Thread-local storage for context
thread_context = threading.local()

# Trade Cache
trade_signal_cache = {}

# Default Leverage size
leverageDefault = -10.0


def make_json_safe(obj):
    if isinstance(obj, dict):
        return {k: make_json_safe(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [make_json_safe(v) for v in obj]
    elif isinstance(obj, datetime.datetime):
        return obj.isoformat()
    return obj

    
# 🔁 Publisher (sync part) – reads redis_notify & publishes
# async def db_to_redis_publisher():
#     r = redis.Redis(host="localhost", port=6379, decode_responses=True)

#     while not stop_event.is_set():
#         try:
#             conn = db_conn.get_connection()
#             cursor = conn.cursor()
#             cursor.execute("SELECT * FROM redis_notify WHERE published = 0 ORDER BY id ASC LIMIT 20")
#             rows = cursor.fetchall()

#             for row in rows:
#                 if row["action_type"] == "delete":
#                     # Row is already deleted, so just send minimal info
#                     message = {
#                         "op": "delete",
#                         "user_cred_id": row["user_cred_id"],
#                         "row": { "id": row["row_id"] }  # no other fields needed
#                     }
#                 else:
#                     data = fetch_row(db_conn, row["table_name"], row["row_id"])
#                     if not data:
#                         continue

#                     key = get_cache_key(row["user_cred_id"], data["status"])
#                     cache_list = trade_signal_cache.get(key, [])
#                     existing_row = next((r for r in cache_list if r["id"] == data["id"]), None)

#                     # if existing_row == data:
#                     #     continue  # ✅ No change → skip publishing

#                     message = {
#                         "op": row["action_type"],
#                         "user_cred_id": row["user_cred_id"],
#                         "row": make_json_safe(data)
#                     }

#                 await r.publish("refresh_trade_signals", json.dumps(message))
#                 print(f"📤 Published {row['action_type']} for trade_id={row['row_id']}")

#                 cursor.execute("DELETE FROM redis_notify WHERE id = %s", (row["id"],))
#                 conn.commit()

#         except Exception as e:
#             print("❌ DB publisher error:", e)

#         time.sleep(1)
        
def add_or_update_trade_cache(user_cred_id, row):
    key = get_cache_key(user_cred_id, row["status"])
    cache_list = trade_signal_cache.setdefault(key, [])
    for i, r in enumerate(cache_list):
        if r["id"] == row["id"]:
            cache_list[i] = row  # update
            return
    cache_list.append(row)
    
def remove_trade_from_cache(user_cred_id, row_id):
    # print(f"us: {user_cred_id} || row_id: {row_id}")
    for key in list(trade_signal_cache):
        if key.startswith(f"{user_cred_id}:"):
            cache_list = trade_signal_cache[key]
            original_len = len(cache_list)
            trade_signal_cache[key] = [row for row in cache_list if row["id"] != row_id]
            if len(trade_signal_cache[key]) < original_len:
                print(f"🗑️ Removed row ID {row_id} from cache {key}")

# async def redis_cache_listener():
#     r = redis.Redis(host="localhost", port=6379, decode_responses=True)
#     pubsub = r.pubsub()
#     await pubsub.subscribe("refresh_trade_signals")
#     print("✅ Subscribed to Redis channel 'refresh_trade_signals'")

#     while not stop_event.is_set():
#         async for msg in pubsub.listen():
#             if msg["type"] != "message":
#                 continue
#             try:
#                 data = json.loads(msg["data"])
#                 user_cred_id = data["user_cred_id"]
#                 row = data["row"]
#                 # print(f"data-op: {data['op']} -- Data: {data}")
#                 if data["op"] == "delete":
#                     # print("here")
#                     remove_trade_from_cache(user_cred_id, row["id"])
#                 else:
#                     add_or_update_trade_cache(user_cred_id, row)
#                     print(f"✅ Live cache updated for {user_cred_id} row ID {row['id']}")
#             except Exception as e:
#                 print("❌ Error in Redis listener:", e)
                
def get_cache_key(user_cred_id, status):
    return f"{user_cred_id}:{status}"

def clear_trade_signal_cache():
    try:
        if isinstance(trade_signal_cache, dict):
            trade_signal_cache.clear()
            print("🧹 Cleared in-memory trade_signal_cache")
        else:
            trade_signal_cache.flushdb()
            print("🧹 Flushed Redis trade_signal_cache")
    except Exception as e:
        print(f"❌ Error clearing cache: {e}")

def fetch_trade_signals(user_cred_id, status):
    key = get_cache_key(user_cred_id, status)
    cached = trade_signal_cache.get(key)
    
    # print("CACHED: ", cached)

    # if cached is not None and len(cached) > 0:
    #     return cached  # ✅ Cache hit

    # print(f"⚠️ No cache found for {key}, falling back to DB...")

    # 🔁 Fetch from DB directly
    conn = db_conn.get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT * FROM opn_trade
                WHERE user_cred_id = %s AND status = %s
            """, (user_cred_id, status))
            results = cursor.fetchall()

            if results:
                trade_signal_cache[key] = results  # ✅ Populate cache
            return results
    finally:
        conn.close()  # ✅ Ensure the connection is closed


# def fetch_trade_signals(user_cred_id, status):
#     conn = db_conn.get_connection()
#     with conn.cursor() as cursor:
#         cursor.execute("""
#             SELECT * FROM opn_trade
#             WHERE user_cred_id = %s AND status = %s
#         """, (user_cred_id, status))
#         results = cursor.fetchall()
#         return results if results else []

def save_trade_history(data: dict):
    try:
        response = requests.post(API_URL, json=data)
        print("🔍 RAW RESPONSE:", response.status_code)
        print("🔍 RESPONSE TEXT:", response.text)
        if response.status_code == 200:
            print("✅ Saved:", response.json())
            return True
        else:
            print("❌ Failed to save:", response.status_code, response.text)
            return False
    except Exception as e:
        print("⚠️ Error:", str(e))
        return False

def update_trade_history(data: dict):
    """
    Sends a flexible update request to your PHP backend.

    data = {
        "token": str,
        "table_name": str,
        "updates": dict,
        "conditions": dict
    }
    """
    try:
        response = requests.post(UPDATE_API_URL, json=data)
        print("🔄 RAW RESPONSE:", response.status_code)
        print("🔄 RESPONSE TEXT:", response.text)
        if response.status_code == 200:
            print("✅ Updated:", response.json())
            return True
        else:
            print("❌ Failed to update:", response.status_code, response.text)
            return False
    except Exception as e:
        print("⚠️ Error:", str(e))
        return False


def create_exchange(exchange_name, api_key, secret, password=None):
    """Create and return a CCXT exchange instance"""
    try:
        exchange_class = getattr(ccxt, exchange_name)
        config = {
            'apiKey': api_key,
            'secret': secret,
            'enableRateLimit': True,
            'options': {
                'type': 'swap',
                'defaultType': 'swap',
            },
        }

        # Add password if provided
        if password:
            config['password'] = password

        return exchange_class(config)
    except AttributeError:
        raise ValueError(f"Exchange '{exchange_name}' not found in CCXT")
    except Exception as e:
        raise Exception(f"Failed to create {exchange_name} exchange: {e}")

def set_thread_context(account_id=None, symbol=None, side=None):
    thread_context.account_id = account_id
    thread_context.symbol = symbol
    thread_context.side = side

def get_thread_context():
    return getattr(thread_context, 'account_id', None), getattr(thread_context, 'symbol', None), getattr(thread_context, 'side', None)

def buffer_print(*args):
    line = ' '.join(str(a) for a in args)
    account_id, symbol, side = get_thread_context()
    if account_id is None or symbol is None or side is None:
        # fallback to immediate print if no context
        with print_lock:
            print(line)
        return

    with buffer_lock:
        symbol_buffers[(account_id, symbol, side)].append(line)

def flush_symbol_buffer():
    account_id, symbol, side = get_thread_context()
    if account_id is None or symbol is None or side is None:
        return

    with buffer_lock:
        lines = symbol_buffers.pop((account_id, symbol, side), [])
    if lines:
        full_message = '\n'.join(lines) + '\n'
        with print_lock:
            print(full_message)

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

def normalize_amount(value, precision):
    scale = 1 / precision
    normalized = int(value * scale) * precision

    # Optional: format to fixed decimal places
    decimals = len(str(precision).split('.')[-1]) if '.' in str(precision) else 0
    return float(f"{normalized:.{decimals}f}")

def adjust_size_to_precision(size, precision):
    # Floors size to nearest allowed precision increment
    return math.floor(size / precision) * precision

def calculateLiquidationTargPrice(_liqprice, _entryprice, _percnt, _round):
    return round_to_sig_figs(_entryprice + (_liqprice - _entryprice) * _percnt, _round)

def get_symbol_lock(symbol):
    with re_symbol_locks_lock:
        if symbol not in re_symbol_locks:
            re_symbol_locks[symbol] = threading.Lock()
        return re_symbol_locks[symbol]
    
def safe_reEnterTrade(exchange, trade_id, symbol, order_side, trigger_price, re_entry_size, order_type, dn_allow_rentry):
    lock = get_symbol_lock(symbol)
    acquired = lock.acquire(blocking=False)
    
    if dn_allow_rentry == 1:
        # buffer_print(f"⏩ Skipping re-entry for {symbol}, already re-entered.")
        pass
        return

    try:
        # Critical section - only one thread per symbol here
        result = reEnterTrade(exchange, trade_id, symbol, order_side, trigger_price, re_entry_size, order_type, dn_allow_rentry)
        if result:
            buffer_print(f"✅ Re-entered trade for {symbol}")
        return result
    finally:
        lock.release()
        
def hedge_position(exchange, trade_id, symbol, order_side, order_price, order_amount, order_type):
    trigger_direction = 'up' if order_side == 'buy' else 'down'
    ensure_hedge_mode(exchange, symbol)
    try:
        # First attempt: without posSide (works in one-way mode)
        order = exchange.create_order(
            symbol=symbol,
            type=order_type,
            side=order_side,
            amount=order_amount,
            price=order_price,
            params={
                'triggerPrice': order_price,
                'triggerType': 'ByMarkPrice',
                'triggerDirection': trigger_direction,
                'closeOnTrigger': False,
                'reduceOnly': False
            }
        )
        buffer_print(f"✅ HEDGE order placed: {order_side} {order_amount} @ {order_price}")
        hedged_order_id = market_order.get('id')
        print("Gotten Hedge OrderId: ", hedged_order_id)
        return hedged_order_id
    except ccxt.BaseError as e:
        error_msg = str(e)
        # Handle specific phemex error for pilot contract
        if 'Pilot contract is not allowed here' in error_msg:
            buffer_print(f"❌ Phemex error: Pilot contract is not allowed for {symbol}. Skipping order.")
            return
        # If failed due to position mode, retry with posSide
        if 'TE_ERR_INCONSISTENT_POS_MODE' in error_msg:
            buffer_print("🔁 Retrying with Hedge (Limit) posSide due to inconsistent position mode...")
            pos_side = 'Long' if order_side == 'buy' else 'Short'
            try:
                order = exchange.create_order(
                    symbol=symbol,
                    type=order_type,
                    side=order_side,
                    amount=order_amount,
                    price=order_price,
                    params={
                        'triggerPrice': order_price,
                        'triggerType': 'ByMarkPrice',
                        'triggerDirection': trigger_direction,
                        'closeOnTrigger': False,
                        'reduceOnly': False,
                        'posSide': pos_side
                    }
                )
                buffer_print(f"✅ HEDGE Limit order (with posSide) placed: {order_side} {order_amount} @ {order_price}")
                hedged_order_id = market_order.get('id')
                print("Gotten Hedge OrderId: ", hedged_order_id)
                return hedged_order_id
            except ccxt.BaseError as e2:
                buffer_print(f"❌ HEDGE Limit order failed even with posSide: {e2}")
                return False
        else:
            buffer_print(f"❌ Error placing HEDGE Limit order: {e}")
            return False


def reEnterTrade(exchange, trade_id, symbol, order_side, order_price, order_amount, order_type, dn_allow_rentry):
    try:
        
        if dn_allow_rentry == 1:
            # buffer_print(f"⏩ Skipping re-entry for {symbol}, already re-entered.")
            pass
            return
        
        # Check if symbol is futures (adjust this check to your actual symbol format)
        if ":USDT" not in symbol:
            buffer_print(f"Skipping re-entry order for non-futures symbol: {symbol}")
            return

        # Fetch balance once
        balance_info = exchange.fetch_balance({'type': 'swap'})
        usdt_balance = balance_info.get('USDT', {}).get('free', 0)

        estimated_cost = order_amount * order_price

        # if usdt_balance < estimated_cost:
        #     buffer_print(f"⚠️ Insufficient USDT balance ({usdt_balance}) for order cost ({estimated_cost}). Skipping order.")
        #     return

        trigger_direction = 'up' if order_side == 'sell' else 'down'
        
        ensure_hedge_mode(exchange, symbol)
        
        # First attempt: without posSide (works in one-way mode)
        order = exchange.create_order(
            symbol=symbol,
            type=order_type,
            side=order_side,
            amount=order_amount,
            price=order_price,
            params={
                'triggerPrice': order_price,
                'triggerType': 'ByMarkPrice',
                'triggerDirection': trigger_direction,
                'closeOnTrigger': False,
                'reduceOnly': False
            }
        )
        buffer_print(f"✅ Re-entry order placed: {order_side} {order_amount} @ {order_price}")
        dn_allow_rentry_checkIn = update_row(db_conn,
            table_name = 'opn_trade',
            updates = {
                'dn_allow_rentry': 1
            },
            conditions = {'id': ('=', trade_id),'symbol': symbol})
        if dn_allow_rentry_checkIn:
            buffer_print(f"🔒🔒 Symbol[{symbol}] rentry access is locked.")
        return True

    except ccxt.BaseError as e:
        error_msg = str(e)
        # Handle specific phemex error for pilot contract
        if 'Pilot contract is not allowed here' in error_msg:
            buffer_print(f"❌ Phemex error: Pilot contract is not allowed for {symbol}. Skipping order.")
            return

        # If failed due to position mode, retry with posSide
        if 'TE_ERR_INCONSISTENT_POS_MODE' in error_msg:
            buffer_print("🔁 Retrying with (Limit) posSide due to inconsistent position mode...")
            pos_side = 'Long' if order_side == 'buy' else 'Short'
            try:
                order = exchange.create_order(
                    symbol=symbol,
                    type=order_type,
                    side=order_side,
                    amount=order_amount,
                    price=order_price,
                    params={
                        'triggerPrice': order_price,
                        'triggerType': 'ByMarkPrice',
                        'triggerDirection': trigger_direction,
                        'closeOnTrigger': False,
                        'reduceOnly': False,
                        'posSide': pos_side
                    }
                )
                buffer_print(f"✅ Re-entry Limit order (with posSide) placed: {order_side} {order_amount} @ {order_price}")
                dn_allow_rentry_checkIn = update_row(db_conn,
                    table_name = 'opn_trade',
                    updates = {
                        'dn_allow_rentry': 1
                    },
                    conditions = {'id': ('=', trade_id),'symbol': symbol})
                if dn_allow_rentry_checkIn:
                    buffer_print(f"🔒🔒 Symbol[{symbol}] (with posSide) rentry access is locked.")
                return True
            except ccxt.BaseError as e2:
                buffer_print(f"❌ Re-entry Limit order failed even with posSide: {e2}")
                return False
        else:
            buffer_print(f"❌ Error placing re-entry Limit order: {e}")
            return False

def update_rentry_count(trade_id, symbol, count):
    try:
        reset_reentry_count = update_row(db_conn,
            table_name='opn_trade',
            updates={'re_entry_count': count},
            conditions={'id': ('=', trade_id), 'symbol': symbol}
        )
        if reset_reentry_count:
            buffer_print(f"✅✅ Symbol[{symbol}] re-entry count is reset to:: ", count)
    except Exception as e:
        print(f"An error occred wile trying to rese Re-entry--count, Error: ", {e})



def cancel_orphan_orders(exchange, symbol, side, trade_id, re_entry_count, order_type='limit'):
    """
    Cancel all open limit and conditional limit orders of the specified side for a symbol,
    assuming the position has already been closed.

    :param exchange: The ccxt exchange object
    :param symbol: Symbol string like 'BTCUSDT'
    :param side: 'buy' or 'sell' — from the original trade signal
    :param order_type: Default to 'limit' — will also include conditional limit orders (LimitIfTouched or MarketIfTouched)
    """
    try:
        open_orders = exchange.fetch_open_orders(symbol)
        if not open_orders:
            return False  # No orders to cancel
        
        for order in open_orders:
            order_side = order['side'].lower()

            # Skip if order type is not limit or conditional limit
            order_type_lower = order['type'].lower()
            if order_type_lower not in ['limit', 'limitiftouched', 'marketiftouched']:
                continue

            # Only cancel orders matching the passed-in side (never opposite)
            if order_side != side.lower():
                continue

            # Determine if this is a conditional limit order
            is_conditional = (order_type_lower == 'limitiftouched' or order_type_lower == 'marketiftouched')

            # For limit orders, cancel only if remaining > 0
            # For conditional limit orders, cancel regardless of remaining (may be zero if untriggered)
            if (order_type_lower == 'limit' and order['remaining'] == 0):
                continue  # skip fully filled/canceled limit orders

            # Proceed to cancel
            buffer_print(f"❌ Cancelling {order_side.upper()} {order_type_lower.upper()} order for {symbol} (position closed)")

            try:
                exchange.cancel_order(order['id'], symbol)
                update_rentry_count(trade_id, symbol, re_entry_count)
                return True
            except Exception as e:
                if "TE_ERR_INCONSISTENT_POS_MODE" in str(e):
                    pos_side_str = "Long" if order_side == "buy" else "Short"
                    buffer_print(f"🔁 Retrying cancel with posSide={pos_side_str}")
                    exchange.cancel_order(order['id'], symbol, {'posSide': pos_side_str})
                    update_rentry_count(trade_id, symbol, re_entry_count)
                    return True
                else:
                    buffer_print(f"⚠️ Error cancelling order: {e}")
                    return False

    except Exception as e:
        buffer_print(f"❌ Global error in cancel_orphan_orders: {e}")  
        return False
    
def monitor_position_and_reenter(
    exchange, user_cred_id, trade_id, symbol, position, lv_size,
    re_entry_count, dn_allow_rentry, hedged, hedge_start, hedge_limit,
    verbose=False, multiplier=1.5
):
    try:
        if not position:
            if verbose:
                buffer_print(f"No open position for {symbol}.")
            return

        # Extract critical values safely
        # liquidation_price = float(position.get('liquidationPrice') or 0)
        entry_price = float(position.get('entryPrice') or 0)
        liquidation_price = entry_price + (entry_price*0.2)
        mark_price = float(position.get('markPrice') or 0)
        contracts = float(position.get('contracts') or 0)
        leverage = float(position.get("leverage") or 1)
        notional = float(position.get('notional') or 0)
        side =  get_position_side(position).lower()

        if not (liquidation_price and entry_price and mark_price):
            return

        # Precision handling
        precision = exchange.markets[symbol]['precision']
        price_sig_digits = count_sig_digits(precision['price'])
        amount_sig_digits = count_sig_digits(precision['amount'])
        amount_precision = precision['amount']
        decimals = count_sig_digits(amount_precision)

        # Closeness to liquidation
        distance_total = abs(entry_price - liquidation_price)
        distance_current = abs(mark_price - liquidation_price)
        closeness = 1 - (distance_current / distance_total) if distance_total else 0

        # Re-entry tier system
        RE_FIRST_STOP = 3
        RE_SECOND_STOP = 6
        RE_THIRD_STOP = 8

        # Raw re-entry size logic
        if re_entry_count <= RE_FIRST_STOP:
            raw_size = contracts
        elif RE_FIRST_STOP < re_entry_count <= RE_SECOND_STOP:
            raw_size = contracts
        elif RE_SECOND_STOP < re_entry_count <= RE_THIRD_STOP:
            raw_size = contracts
        else:
            raw_size = contracts / 4 if re_entry_count >= 16 else contracts

        re_entry_size = normalize_amount(raw_size, amount_precision)

        if verbose:
            buffer_print(
                f"[{symbol}] Side: {side}, Entry: {entry_price}, Mark: {mark_price}, "
                f"Liquidation: {liquidation_price}, Closeness: {closeness * 100:.1f}%"
            )

        open_orders = exchange.fetch_open_orders(symbol)
        same_side = 'buy' if side == 'long' else 'sell'
        order_side = 'sell' if side == 'short' else 'buy'
        hedge_order_side = 'buy' if side == 'short' else 'sell'

        trigger_price = calculateLiquidationTargPrice(
            entry_price, liquidation_price, 0.2, price_sig_digits
        )

        order_amount = round_to_sig_figs(
            ((notional / leverage) * multiplier) / mark_price, amount_sig_digits
        )

        hedge_order_amount = normalize_amount(contracts / 4.0, amount_precision)
        hedge_side_int = 0 if hedge_order_side == 'buy' else 1
        trail_thresh = 0.20
        profit_target_distance = 0.01

        if hedged == 200:
            if verbose:
                buffer_print(f"Trade is done, Hedge logic stoped -- TradeId: {trade_id}")
            return

        # 🔁 Hedge re-entry trigger
        if re_entry_count >= hedge_start and hedged == 0:
            hedge_order = place_entry_and_liquidation_limit_order(
                exchange, symbol, hedge_order_side, hedge_order_amount, leverageDefault
            )
            if hedge_order:
                hedged_trade_data = {
                    "strategy_type": "hedge",
                    "user_cred_id": user_cred_id,
                    "child_to": trade_id,
                    "trade_signal": -900000,
                    "order_id": hedge_order,
                    "symbol": symbol,
                    "trade_type": hedge_side_int,
                    "amount": hedge_order_amount,
                    "leverage": leverageDefault,
                    "trail_threshold": trail_thresh,
                    "profit_target_distance": profit_target_distance,
                    "cum_close_threshold": 1.0,
                    "cum_close_distance": 0.5,
                    "trade_done": 0,
                    "re_entry_count": -1,
                    "hedged": 0,
                    "hedge_start": 3,
                    "status": 1
                }
                if update_row(
                    db_conn, 'opn_trade', {'hedged': 1}, {'id': ('=', trade_id), 'symbol': symbol}
                ):
                    print(f"✅ UPDATED 'hedged' to 1 for tradeId: {trade_id}")
                if insert_trade_data(db_conn, hedged_trade_data):
                    print(f"🗃️🗃️ Stored Hedge Trade {hedge_order} Successfully!")

        # 🛡️ Limit hedge order logic
        is_hedged = has_opposite_position_open(exchange, symbol, order_side)
        is_limit_hedge = has_limit_hedge_open(db_conn, "hedge_limit", user_cred_id, trade_id, hedge_side_int)

        if hedged == 1 and not (is_hedged or is_limit_hedge) and hedge_limit == 0:
            limit_hedge_order = place_limit_buy_above_market(
                exchange, symbol, hedge_order_side, "limit", hedge_order_amount, pip_count=55
            )
            if limit_hedge_order:
                limit_hedge_data = {
                    "user_id": user_cred_id,
                    "trade_id": trade_id,
                    "order_id": limit_hedge_order,
                    "trade_type": hedge_side_int
                }
                if insert_hedge_limit_data(db_conn, limit_hedge_data):
                    print(f"🛡️ Hedge limit for {limit_hedge_data} inserted successfully!")
                    if update_row(
                        db_conn, 'opn_trade', {'hedge_limit': 1},
                        {'id': ('=', trade_id), 'symbol': symbol}
                    ):
                        print(f"✅ UPDATED 'hedge_limit' to 1 for tradeId: {trade_id}")
                        return

        # ⛔ Orphan order handling
        for o in open_orders:
            if o['side'].lower() == same_side:
                order_type = o['type'].lower()
                if order_type in ['limit', 'limitiftouched', 'marketiftouched'] and o['amount'] != re_entry_size:
                    if verbose:
                        buffer_print(
                            f"[{symbol}] Detected same-side {order_type.upper()} order "
                            f"with mismatched size {o['amount']} ≠ expected {re_entry_size}. Cancelling."
                        )

                    cancel_orphan_orders(exchange, symbol, same_side, trade_id, re_entry_count - 1, 'limit')

                    if dn_allow_rentry == 1:
                        if update_row(
                            db_conn, 'opn_trade', {'dn_allow_rentry': 0},
                            {'id': ('=', trade_id), 'symbol': symbol}
                        ):
                            buffer_print(f"✅✅ Symbol[{symbol}] re-entry access is unlocked.")
                    return

        # ⏭️ Skip if a valid order already exists
        if any(
            o['side'].lower() == same_side and 
            o['type'].lower() in ['limit', 'limitiftouched', 'marketiftouched']
            for o in open_orders
        ):
            if verbose:
                buffer_print(f"[{symbol}] Same-side limit or conditional limit order exists. Skipping re-entry.")
            return
        
        if re_entry_count <= hedge_start and hedged == 0:
            # ✅ Main re-entry execution
            if dn_allow_rentry == 1:
                if update_row(
                    db_conn, 'opn_trade', {'dn_allow_rentry': 0},
                    {'id': ('=', trade_id), 'symbol': symbol}
                ):
                    buffer_print(f"✅✅ Symbol[{symbol}] re-entry access is unlocked.")

            if safe_reEnterTrade(exchange, trade_id, symbol, order_side, trigger_price, re_entry_size, 'limit', dn_allow_rentry):
                if verbose:
                    buffer_print(f"✅✅✅ Re-entered for {symbol} ✅✅✅")

                updates = {
                    'lv_size': re_entry_size,
                    're_entry_count': re_entry_count + 1
                }
                if re_entry_count > 0:
                    updates['profit_target_distance'] = 0.10

                update_row(
                    db_conn, 'opn_trade', updates,
                    {'id': ('=', trade_id), 'symbol': symbol}
                )

            # ℹ️ Informational output based on proximity
            if closeness >= 0.8:
                if verbose:
                    buffer_print(f"⚠️ Re-entry trigger initiated for {symbol}.")
            else:
                if verbose:
                    buffer_print(f"✅ Not close enough for re-entry on {symbol}.")

    except ccxt.ExchangeError as e:
        buffer_print(f"Exchange error for {symbol}: {e}")

    except KeyError as ke:
        buffer_print(f"Missing key in {symbol} position data: {ke}")

    except Exception as e:
        buffer_print(f"Unexpected error in monitor_position_and_reenter for {symbol}: {e}")
        
        
def monitor_hedge_position(exchange, user_id, trade_id, symbol, hedged, status, trade_type=0):
    # Get hedge limit row data from the database
    # trade_type=0 for buy
    hd_limit_data_raw = fetch_row_details(db_conn, "hedge_limit", user_id, trade_id, trade_type)
    
    if not hd_limit_data_raw:
        # 🛑 No matching hedge limit entry found — exit early
        return

    # 🧼 Make the data JSON-safe
    hd_limit_data = make_json_safe(hd_limit_data_raw)
    
    # print("Raw-data: ", hd_limit_data)
    
    order_id = hd_limit_data['order_id']

    # Remove pending orders if SELL trade is already closed
    if hedged == 200 and status == -2:
        side_cancel = "long" if trade_type == 0 else "short"
        result = cancel_existing_stop_order(exchange, symbol, order_id, side_cancel)
        if result:
            buffer_print(f"Closed PENDING {side_cancel.upper()} order, since main(SELL-HEDGED) trade is out")
    
    check_limit_order = check_if_order_filled_and_store(exchange, user_id, trade_id, order_id, symbol, trade_type)
    
    # 🧠 Cancel the order if market price falls below limit
    cancel_to_reset_order = cancel_if_price_falls_below_limit(exchange, order_id, symbol)
    if cancel_to_reset_order:
        print(f"\t\t\tPending limit order {order_id} canceled.. This is for reset of the order at a new price!")
        is_deleted = delete_row(db_conn,
            table_name='hedge_limit',
            conditions={'trade_id': trade_id, 'order_id': order_id}
        )
        if is_deleted:
            print(f"✅ Trade: {order_id} - {trade_id} deleted (cancel_to_reset_order did this).")
        else:
            print(f"⚠️ Error Occured while deleting trade: {order_id} - {trade_id} (cancel_to_reset_order did this).")
            
            
def parse_order_data(order):
    """
    Extracts order_status, filled_price, filled_amount, filled_time
    from CCXT order — supports both normalized and raw formats (e.g., Phemex).
    """
    
    # print("oOo: ", order)
    
    info = order.get('info', {})
    rows = info.get('rows')

    # Default normalized structure
    order_status = order.get('status', '')
    filled_price = order.get('average')
    filled_amount = order.get('filled')
    filled_time = order.get('timestamp')

    if isinstance(rows, list) and rows:
        row = rows[0]
        order_status = row.get('ordStatus', order_status).lower()
        filled_amount = filled_amount or float(row.get('cumQtyRq', 0))
        if not filled_time and 'transactTimeNs' in row:
            filled_time = int(int(row['transactTimeNs']) / 1e6)

    return order_status.lower(), filled_price, filled_amount, filled_time

def check_if_order_filled_and_store(exchange, user_id, trade_id, order_id, symbol, trade_type):
    """
    Checks if a limit order is filled and inserts order info into the database.
    """
    try:
        order = exchange.fetch_order(order_id, symbol)
        order_status, filled_price, filled_amount, filled_time = parse_order_data(order)

        if not order_status:
            print(f"⚠️ Could not determine order status for {order_id}.")
            return None

        if order_status in ['filled', 'closed']:
            trail_thresh = 0.20
            profit_target_distance = 0.01
            print(f"✅ Order {order_id} FILLED at {filled_price} for {filled_amount}")

            hedged_limit_taken_data = {
                "strategy_type": "hedge",
                "user_cred_id": user_id,
                "child_to": trade_id,
                "trade_signal": -900000,
                "order_id": order_id,
                "symbol": symbol,
                "trade_type": trade_type,
                "amount": filled_amount,
                "leverage": leverageDefault,
                "trail_threshold": trail_thresh,
                "profit_target_distance": profit_target_distance,
                "cum_close_threshold": 1.0,
                "cum_close_distance": 0.5,
                "trade_done": 0,
                "re_entry_count": -1,
                "hedged": 0,
                "hedge_start": 3,
                "status": 1
            }

            if insert_trade_data(db_conn, hedged_limit_taken_data):
                print(f"\t✅😎 Inserted limit trade {symbol} - {order_id} into opn_table.")
                if delete_row(db_conn, 'hedge_limit', {'trade_id': trade_id, 'order_id': order_id}):
                    print(f"✅ Deleted order {order_id} from hedge_limit.")
                    if update_row(db_conn, 'opn_trade', {'hedge_limit': 0}, {'id': ('=', trade_id), 'symbol': symbol}):
                        print(f"✅ Updated hedge_limit=0 for trade {trade_id}")
                else:
                    print(f"⚠️ Error deleting order {order_id} from hedge_limit.")

            return order

        elif order_status in ['canceled', 'rejected', 'deactivated']:
            print(f"⚠️ Order {order_id} was {order_status.upper()}. Deleting from hedge_limit.")
            if delete_row(db_conn, 'hedge_limit', {'trade_id': trade_id, 'order_id': order_id}):
                print(f"✅ Deleted order {order_id} from hedge_limit.")
                if update_row(db_conn, 'opn_trade', {'hedge_limit': 0}, {'id': ('=', trade_id), 'symbol': symbol}):
                    print(f"✅ Updated hedge_limit=0 for trade {trade_id}")
            else:
                print(f"⚠️ Error deleting order {order_id} from hedge_limit.")
        else:
            # print(f"⏳ Order {order_id} is still OPEN... waiting.")
            pass

        return None

    except Exception as e:
        print(f"❌ Error checking or storing filled order: {e}")
        return None
    
def cancel_if_price_falls_below_limit(exchange, order_id, symbol, pip_count=5):
    try:
        order = exchange.fetch_order(order_id, symbol)
        if not order:
            print(f"⚠️ Order ID {order_id} not found.")
            return False

        # Parse order data (status, etc.)
        order_status, _, _, _ = parse_order_data(order)

        # Get price and side from normalized or raw structure
        info = order.get('info', {})
        rows = info.get('rows', [])
        if isinstance(rows, list) and rows:
            row = rows[0]
            limit_price = float(order.get('price') or row.get('priceRp', 0))
            side = order.get('side') or row.get('side', '').lower()
            symbol = order.get('symbol') or row.get('symbol', symbol)
        else:
            limit_price = float(order.get('price'))
            side = order.get('side').lower()
            symbol = order.get('symbol')

        # Get pip size
        markets = exchange.load_markets()
        market = markets[symbol]
        pip_size = market['precision']['price']
        pip_size_count = count_sig_digits(pip_size)
        if not pip_size:
            raise ValueError(f"⚠️ Could not determine pip size for symbol {symbol}")

        # Current price
        ticker = exchange.fetch_ticker(symbol)
        current_price = ticker.get('mark', ticker.get('last'))
        if current_price is None:
            raise ValueError("⚠️ Could not fetch current price.")

        # Calculate initial threshold price
        threshold_price = round(limit_price - (pip_size * pip_count), pip_size_count)

        # Ensure threshold_price is always below current price
        if threshold_price >= current_price:
            threshold_price = round(current_price - (pip_size * pip_count), pip_size_count)

        # Debug print
        # print(f"Limit price: {limit_price}, Current price: {current_price}, Threshold price: {threshold_price}")

        # Cancel if price has fallen below threshold (for buy orders)
        if side == 'buy' and current_price <= threshold_price:
            side_cancel = "long"
            result = cancel_existing_stop_order(exchange, symbol, order_id, side_cancel)
            print(f"||❌|| Limit order cancelled: Market price {current_price} fell below Threshold price{threshold_price}:: Limit price: {limit_price}")
            return True
        else:
            print(f"✅ Order kept: Market price is {current_price}, threshold is {threshold_price}")
            return False

    except Exception as e:
        print(f"❌ Error checking/cancelling order: {e}")
        return False


def cancel_existing_stop_order(exchange, symbol, order_id, side):
    try:
        cancel_order = exchange.cancel_order(order_id, symbol=symbol)
        buffer_print(f"❌ Canceled previous stop-loss {order_id} (one-way) symol[{symbol}]")
        return True
    except Exception as e:
        if "TE_ERR_INCONSISTENT_POS_MODE" in str(e):
            try:
                params = {'posSide': 'Long' if side == 'long' else 'Short'}
                cancel_order = exchange.cancel_order(order_id, symbol=symbol, params=params)
                buffer_print(f"❌ Canceled stop-loss {order_id} (with posSide) symol[{symbol}]")
                return True
            except Exception as e2:
                buffer_print(f"⚠️ Still failed with posSide symol[{symbol}]: {e2}")
                return False
        else:
            buffer_print(f"⚠️ Cancel failed: {e}")
            return False

        # if cancel_order:
            # update_trail_order('')
    return False

def closeAllPosition(exchange, positions, trade_id, symbol):
    for pos in positions:
        if pos['symbol'] != symbol:
            continue

        side = get_position_side(pos).lower()  # should return 'long' or 'short'
        contracts = float(pos['contracts'])
        if contracts <= 0:
            continue

        posSide = 'Long' if side == 'long' else 'Short'
        order_side = 'sell' if side == 'long' else 'buy'  # what we need to do to close it
        params = {'posSide': posSide}

        try:
            # Try to close SHORT positions (i.e., sell-side positions) first
            if side == 'short':
                buffer_print(f"📉 Closing SHORT position on {symbol} ({contracts} contracts)")
                order = exchange.create_order(
                    symbol=symbol,
                    type='market',
                    side='buy',  # buy to close short
                    amount=contracts,
                    params={'posSide': 'Short'}
                )

                if order:
                    if update_row(
                        db_conn, 'opn_trade', {'hedged': 200}, {'id': ('=', trade_id), 'symbol': symbol}
                    ):
                        print(f"✅ TRADE DONE ( UPDATED 'hedged' to 200 for tradeId: {trade_id})")

        except Exception as e:
            buffer_print(f"⚠️ Failed to close SHORT position on {symbol}: {e}")

    # Now loop again to close LONG positions
    for pos in positions:
        if pos['symbol'] != symbol:
            continue

        side = get_position_side(pos).lower()
        contracts = float(pos['contracts'])
        if contracts <= 0:
            continue

        if side == 'long':
            try:
                buffer_print(f"📈 Closing LONG position on {symbol} ({contracts} contracts)")
                exchange.create_order(
                    symbol=symbol,
                    type='market',
                    side='sell',  # sell to close long
                    amount=contracts,
                    params={'posSide': 'Long'}
                )
            except Exception as e:
                buffer_print(f"⚠️ Failed to close LONG position on {symbol}: {e}")


def create_stop_order(exchange, symbol, side, contracts, new_stop_price):
    params_common = {
        'stopPx': new_stop_price,
        'triggerType': 'ByMarkPrice',
        'triggerDirection': 1 if side == 'long' else 2,
        'reduceOnly': True,
        'closeOnTrigger': True,
        'timeInForce': 'GoodTillCancel',
    }
    
    ensure_hedge_mode(exchange, symbol)

    # Try hedge mode first
    try:
        order = exchange.create_order(
            symbol=symbol,
            type='stop',
            side='sell' if side == 'long' else 'buy',
            amount=contracts,
            price=None,
            params={**params_common, 'positionIdx': 1 if side == 'long' else 2, 'posSide': 'Long' if side == 'long' else 'Short'}
        )
        buffer_print(f"✅ Stop-loss set at {new_stop_price:.4f} (hedge mode)")
        return order
    except Exception as e:
        buffer_print(f"⚠️ Hedge mode failed: {e}")

    # Fallback to one-way mode
    try:
        order = exchange.create_order(
            symbol=symbol,
            type='stop',
            side='sell' if side == 'long' else 'buy',
            amount=contracts,
            price=None,
            params=params_common
        )
        buffer_print(f"✅ Stop-loss set at {new_stop_price:.4f} (one-way mode)")
        return order
    except Exception as e2:
        buffer_print(f"❌ Both order attempts failed: {e2}")
        return None
    

def get_min_leverage(exchange, symbol):
    markets = exchange.load_markets()
    market = markets.get(symbol)
    if market:
        return market.get("limits", {}).get("leverage", {}).get("min")
    return None

def set_phemex_leverage(exchange, trade_id, re_entry_count, symbol, leverage=None, long_leverage=None, short_leverage=None, side=None):
    clean_symbol = symbol.split(':')[0].replace('/', '')  # BIDUSDT format
    
    path = 'g-positions/leverage'
    method = 'PUT'
    
    # Compose query params as strings (required by API)
    params = {
        'symbol': clean_symbol,
    }

    if side is None:
        print("Side is None, check trade side")
        return
    
    if leverage is not None:
        params['leverageRr'] = str(leverage)  # One-way mode leverage
    
    if long_leverage is not None and short_leverage is not None:
        params['longLeverageRr'] = str(long_leverage)
        params['shortLeverageRr'] = str(short_leverage)
    
    try:
        response = exchange.fetch2(path, 'private', method, params)
        if response:
            cancel_orphan_orders(exchange, symbol, side, trade_id, re_entry_count-1, 'limit')
        print(f"Set leverage response: {response}")
    except Exception as e:
        if "TE_ERR_INVALID_LEVERAGE" in str(e):
            minLevegrage = -abs(get_min_leverage(exchange, symbol))
            print("Retrying with minimum leverage: ", minLevegrage)
            if minLevegrage is None:
                print("Min Leverage is None")
                return
            params['leverageRr'] = str(minLevegrage)  # One-way mode leverage
            try:
                response = exchange.fetch2(path, 'private', method, params)
                if response:
                    cancel_orphan_orders(exchange, symbol, side, trade_id, re_entry_count-1, 'limit')
                print(f"Set leverage response: {response}")
            except Exception as e:
                print(f"⚠️ Could not set min leverage also: {e}")
        print(f"⚠️ Could not set leverage: {e}")
        
def GetHedgeFloatingPnl(exchange, all_position, symbol, hedge_side="long"):
    floating_hedge_position = [
        pos for pos in all_position
        if pos.get('symbol') == symbol
        and get_position_side(pos).lower() == hedge_side.lower()
        and (float(pos.get('info', {}).get('size', 0)) > 0 or pos.get('contracts', 0) > 0)
    ]
    if not floating_hedge_position:
        print("⚠️ No matching position found.")
        return None, None
    hedge_position = floating_hedge_position[0]
    entry_price = float(hedge_position.get('entryPrice') or 0)
    mark_price = float(hedge_position.get('markPrice') or 0)
    sideRl =  get_position_side(hedge_position)
    contracts = float(hedge_position.get('contracts') or 0)
    unrealized_pnl = (mark_price - entry_price) * contracts if hedge_side == 'long' else (entry_price - mark_price) * contracts
    realized_pnl = float(hedge_position["info"].get('curTermRealisedPnlRv') or 0)
    total_pnl = unrealized_pnl + realized_pnl
    return total_pnl

def canClosePosition(cummulative_profit, close_threshold, break_threshold):
    # print("Cumulative: ", cummulative_profit)
    if cummulative_profit <= close_threshold and cummulative_profit >= (break_threshold - 0.2):
        return True
    return False

def updateCumTrailThreshold(trade_id, symbol, cummulative_profit, close_threshold, break_threshold):
    if cummulative_profit >= close_threshold:
        newCumCloseThreshold = close_threshold + break_threshold
        cum_close_threshold_update = update_row(db_conn,
            table_name = 'opn_trade',
            updates = {
                'cum_close_threshold': newCumCloseThreshold
            },
            conditions = {'id': ('=', trade_id),'symbol': symbol})
        if cum_close_threshold_update:
            print(f"Updated Cummulative position closethreshold to {newCumCloseThreshold}")

def trailing_stop_logic(exchange, position, user_id, trade_id, trade_order_id, trail_order_id, trail_theshold, profit_target_distance, breath_stop, breath_threshold, closeThreshold, breakThreshold, re_entry_count, hedge, all_position):
    symbol = position.get('symbol')
    entry_price = float(position.get('entryPrice') or 0)
    mark_price = float(position.get('markPrice') or 0)
    sideRl =  get_position_side(position)
    side = sideRl.lower()
    sideNml = position["info"]["side"]
    leverage = float(position.get("leverage") or 1)
    contracts = float(position.get('contracts') or 0)
    margin_mode = position.get('marginMode') or position['info'].get('marginType')
    set_thread_context(user_id, symbol, side)
    if not entry_price or not mark_price or side not in ['long', 'short'] or contracts <= 0:
        return
    
    if margin_mode.lower() != "cross":
        pos_mode = position['info'].get('posMode', '').lower()
        print(f"🛑🛑🛑🛑Symbol: [{symbol}] side: {sideRl}, posMode: {pos_mode} | posSide: {position['info'].get('posSide', '')}")
        if pos_mode == 'oneway':
            set_phemex_leverage(exchange, trade_id, re_entry_count, symbol, leverage=leverageDefault, side=sideNml)
        elif pos_mode == 'hedged':
            set_phemex_leverage(exchange, trade_id, re_entry_count, symbol, long_leverage=leverageDefault, short_leverage=leverageDefault, side=sideNml)

    if -abs(leverage) > leverageDefault and -abs(leverage) <= -1:
        # Depending on mode, set leverage appropriately as above
        pos_mode = position['info'].get('posMode', '').lower()
        print(f"🛑🛑Symbol: [{symbol}] side: {sideRl}, posMode: {pos_mode} | Leverage: {-abs(leverage)} | posSide: {position['info'].get('posSide', '')}")
        if pos_mode == 'oneway':
            set_phemex_leverage(exchange, trade_id, re_entry_count, symbol, leverage=leverageDefault, side=sideNml)
        elif pos_mode == 'hedged':
            set_phemex_leverage(exchange, trade_id, re_entry_count, symbol, long_leverage=leverageDefault, short_leverage=leverageDefault, side=sideNml)

    change = (mark_price - entry_price) / entry_price if side == 'long' else (entry_price - mark_price) / entry_price
    profit_distance = change * leverage
    unrealized_pnl = (mark_price - entry_price) * contracts if side == 'long' else (entry_price - mark_price) * contracts
    realized_pnl = float(position["info"].get('curTermRealisedPnlRv') or 0)
    total_pnl = unrealized_pnl + realized_pnl
    
    # Set default values so they're always defined
    closedHedgeProfit = 0.0
    floatHedgeProfit = 0.0
    CummulativeProfit = total_pnl  # Start from base PnL
    
    if hedge == 1:
        # Cummulative logic in here
        closedHedgeProfit = GetHedgeSummationProfit(db_conn, trade_id)
        # print("closedhedgeprofit: ", closedHedgeProfit)
        floatHedgeProfit = GetHedgeFloatingPnl(exchange, all_position, symbol, hedge_side="long") if has_opposite_position_open(exchange, symbol, sideNml.lower()) else 0.0
        
        CummulativeProfit = total_pnl + closedHedgeProfit + floatHedgeProfit
        cum_break_distance = closeThreshold - breakThreshold
        
        updateCumTrailThreshold(trade_id, symbol ,CummulativeProfit, closeThreshold, breakThreshold)
        
        # print("neww: ", canClosePosition(CummulativeProfit, closeThreshold, cum_break_distance))
        
        if canClosePosition(CummulativeProfit, closeThreshold, cum_break_distance):
            cumulative_close = closeAllPosition(exchange, all_position, trade_id, symbol)
            if cumulative_close:
                buffer_print(f"Cumulative trade is closed for trade {trade_id} @ a profit of {CummulativeProfit}")
                # hedge_trade_deleted = delete_row(db_conn,
                #     table_name='opn_trade',
                #     conditions={'child_to': trade_id}
                # )
                # if hedge_trade_deleted:
                #     print(f"✅ HEDGE trades for: {trade_id} deleted.")
                # else:
                #     print(f"⚠️ Error occred while deleting Hedge trades for: {trade_id} deleted.")
                return

    # buffer_print(f"\n📈💰 {symbol} ({side.upper()}) | Leverage: {leverage} | Contract(Amount): {contracts} | MarginMode: {margin_mode}")
    # buffer_print(f"Profit-Distance: {profit_distance}, PNL → Unrealized: {unrealized_pnl:.4f}, [- Realized: {realized_pnl:.4f}, Total: {total_pnl:.4f}]")
    # if hedge == 1:
    #     buffer_print(f"\n[- closedHedgeProfit: {closedHedgeProfit:.4f}-[- floatHedgeProfit: {floatHedgeProfit:.4f}]([- CummulativeProfit: {CummulativeProfit:.4f}])")

    if total_pnl <= 0.001:
        # print(f"{symbol} here")
        if trail_order_id:
            cancel_conditional_order = cancel_existing_stop_order(exchange, symbol, trail_order_id, side)
            if cancel_conditional_order:
                trailing_update = update_row(db_conn,
                    table_name = 'opn_trade',
                    updates = {
                        'trail_order_id': "",
                        'trail_threshold': 0.20,
                        'profit_target_distance': 0.01,
                        'trade_done': 0
                    },
                    conditions = {'id': ('=', trade_id),'symbol': symbol})
                if trailing_update:
                    print("No more trailing (for now) - Trade Order Id: ", trade_order_id)
                    update_trade_history({
                        "token": TOKEN,
                        "table_name": "trade_history",
                        "updates": {
                            "trail_threshold": trail_theshold,
                            "profit_target_distance": profit_target_distance,
                            "trade_done": 0
                        },
                        "conditions": {
                            "order_id": ("=", trade_order_id),
                            "symbol": symbol
                        }
                    })
        return
    
    if profit_distance >= trail_theshold:
        # buffer_print(f"Thresh val: {trail_theshold}")
        new_stop_price_distance = mark_price * profit_target_distance
        new_stop_price = (mark_price - new_stop_price_distance) if side == 'long' else (mark_price + new_stop_price_distance)
        # new_stop_price = entry_price * (1 + profit_target_distance / leverage) if side == 'long' else entry_price * (1 - profit_target_distance / leverage)
        if (side == 'long' and new_stop_price <= entry_price) or (side == 'short' and new_stop_price >= entry_price):
            # buffer_print(f"❌ Invalid stop-loss price {new_stop_price:.4f} vs entry {entry_price}")
            pass
            return

        if trail_order_id != '':
            cancel_conditional_order = cancel_existing_stop_order(exchange, symbol, trail_order_id, side)
            if cancel_conditional_order:
                print(f"Removing old conditional🤗🤗: {symbol} -> {trail_order_id}")
                
        order = create_stop_order(exchange, symbol, side, contracts, new_stop_price)
        if order:
            new_trail_order_id = order['id']
            new_trail_theshold = trail_theshold + breath_threshold
            new_profit_target_distance = breath_stop
            trailing_update = update_row(db_conn,
                table_name = 'opn_trade',
                updates = {
                    'trail_order_id': new_trail_order_id,
                    'trail_threshold': new_trail_theshold,
                    'profit_target_distance': new_profit_target_distance,
                    'trade_done': 1
                },
                conditions = {
                    'id': ('=', trade_id),
                    'symbol': symbol
                }
            )
            if trailing_update:
                print("Trade Order Id: ", trade_order_id)
                update_trade_history({
                    "token": TOKEN,
                    "table_name": "trade_history",
                    "updates": {
                        "trail_threshold": new_trail_theshold,
                        "profit_target_distance": new_profit_target_distance,
                        "trade_done": 1
                    },
                    "conditions": {
                        "order_id": ("=", trade_order_id),
                        "symbol": symbol
                    }
                })
            # update_trailing_data(trade_id, symbol, new_trail_order_id, new_trail_theshold, new_profit_target_distance, 1)
            # save_trailing_data(symbol, trailing_data, side)
            
def find_closing_position(open_execSeq, side, symbol, closed_positions):
    open_execSeq = int(open_execSeq)
    valid_candidates = []

    for p in closed_positions:
        # print(f"pp: {p}")
        try:
            pos_execSeq = int(p['info']['execSeq'])
            pos_size = float(p['info'].get('size', '0'))
            
            # print(f"Side: {get_position_side(p)}")
            # print(f"Open execseq: {pos_execSeq}")
            # print(f"DataExecq: {pos_execSeq}")

            if (
                p['symbol'] == symbol
                and get_position_side(p) == side.lower()
                and pos_size == 0
                and pos_execSeq >= open_execSeq
            ):
                valid_candidates.append((pos_execSeq, p))
        except (KeyError, ValueError, TypeError):
            continue  # skip invalid entries

    if not valid_candidates:
        return None

    # Sort by smallest difference from open_execSeq
    closest = min(valid_candidates, key=lambda x: x[0] - open_execSeq)
    return closest[1]

def CorrectTradeDetailsFunction(exchange, saved_open_symbol, saved_open_side, saved_open_execSeq, prevCumClosedPnl, closed_positions):
    # Find the correct closed match
    matched_closed = find_closing_position(saved_open_execSeq, saved_open_side, saved_open_symbol, closed_positions)
    # Output
    if matched_closed:
        print("Matched Closed Position: ", closed_positions)
        newExecSeq = matched_closed.get('info', {}).get('execSeq', '')
        newCumClosedPnl = matched_closed.get('info', {}).get('cumClosedPnlRv')
        
        # print(f"NuCum: {newCumClosedPnl} and PrevCum: {prevCumClosedPnl}")
        realizedPnl = float(newCumClosedPnl) - float(prevCumClosedPnl)
        # print(f"pnl: {realizedPnl}")
        return newCumClosedPnl, realizedPnl
    else:
        print("No matching closed position found. ", closed_positions)
        return None
    
def get_position_side(pos):
    if pos.get('info', {}).get('posMode') == 'Hedged':
        return pos.get('info', {}).get('posSide', '').lower()
    return pos.get('side', '').lower()

def mark_trade_signal_closed_if_position_closed(exchange, strategy_type, symbol, trade_order_id, trade_id, side, re_entry_count, execSeq, prevCumClosedPnl, positionst):
    """
    Checks if a position with the given side is still open for the symbol.
    If not, updates trade_signal.status = 0 for that trade_id.

    :param symbol: Symbol like 'BTCUSDT'
    :param trade_id: The ID of the trade_signal row
    :param side: 'buy' or 'sell' from trade signal
    :param positionst: List of positions from exchange
    """
    # print(f"SIDE2: {side}")
    target_side = 'long' if side.lower() == 'buy' else 'short'
    

    is_open = any(
        pos['symbol'] == symbol and
        pos.get('contracts', 0) > 0 and
        get_position_side(pos) == target_side.lower()
        for pos in positionst
    )  

    if not is_open:
        if strategy_type == "initial":
            # print(f"IsOpen: {is_open}")
            cancl_orphan_ordrs = cancel_orphan_orders(exchange, symbol, side, trade_id, re_entry_count-1, 'limitiftouched')
            if cancl_orphan_ordrs:
                # print("hereddd")
                backup_trade_final = update_trade_history ({
                    "token": TOKEN,
                    "table_name":"trade_history",
                    "updates":{'status': 0},
                    "conditions":{'order_id': trade_order_id}
                })
                if backup_trade_final:
                    buffer_print(f"🔄 Symbol {symbol} [{side.upper()}] closed. Marked trade_signal ID {trade_id} as status=0.")
                    newCumClosedPnl, realizedPnl = CorrectTradeDetailsFunction(exchange, symbol, target_side, execSeq, prevCumClosedPnl, positionst)
                    update_to_correct_details = update_row(db_conn,table_name='opn_trade',updates={'realizedPnl': realizedPnl, 'currCumClosedPnl': newCumClosedPnl, 'status':-2},conditions={'id': ('=', trade_id), 'symbol': symbol})
                    if update_to_correct_details:
                        buffer_print(f"✅ HEDGE trade {trade_id} details corrected:  [- Realized PnL: {realizedPnl} -] [- New Cummuative Closed PnL {newCumClosedPnl} -]")
                        # is_deleted = delete_row(db_conn,
                        #     table_name='opn_trade',
                        #     conditions={'id': trade_id}
                        # )
                        # if is_deleted:
                        #     print(f"✅ Trade: {trade_order_id} deleted.")
                        # else:
                        #     print(f"⚠️ Error occred while deleting Trade: {trade_order_id} deleted.")
                        return
                    else:
                        buffer_print(f"⚠️ HEDGE trade: {trade_order_id} or {trade_id} has an issue updating to the correct trade details: [- Realized PnL: {realizedPnl} -] [- New Cummuative Closed PnL {newCumClosedPnl} -]")
            else:
                buffer_print(f"An Error occured while updating backup data for {symbol} [{side.upper()}]. Marked trade_signal ID {trade_id} as status=0")
                buffer_print(f"Retrying [Deleting trade {trade_order_id}]....")
                newCumClosedPnl, realizedPnl = CorrectTradeDetailsFunction(exchange, symbol, target_side, execSeq, prevCumClosedPnl, positionst)
                update_to_correct_details = update_row(db_conn,table_name='opn_trade',updates={'realizedPnl': realizedPnl, 'currCumClosedPnl': newCumClosedPnl, 'status':-2},conditions={'id': ('=', trade_id), 'symbol': symbol})
                if update_to_correct_details:
                    buffer_print(f"✅ HEDGE trade {trade_id} details corrected:  [- Realized PnL: {realizedPnl} -] [- New Cummuative Closed PnL {newCumClosedPnl} -]")
                    # is_deleted = delete_row(db_conn,
                    #     table_name='opn_trade',
                    #     conditions={'id': trade_id}
                    # )
                    # if is_deleted:
                    #     print(f"✅ Trade2: {trade_order_id} deleted.")
                    # else:
                    #     print(f"⚠️ Error2 occred while deleting Trade: {trade_order_id} deleted.")
                    return
                else:
                    buffer_print(f"⚠️ HEDGE trade: {trade_order_id} or {trade_id} has an issue updating to the correct trade details: [- Realized PnL: {realizedPnl} -] [- New Cummuative Closed PnL {newCumClosedPnl} -]")
        elif strategy_type == "hedge":
            newCumClosedPnl, realizedPnl = CorrectTradeDetailsFunction(exchange, symbol, target_side, execSeq, prevCumClosedPnl, positionst)
            update_to_correct_details = update_row(db_conn,table_name='opn_trade',updates={'realizedPnl': realizedPnl, 'currCumClosedPnl': newCumClosedPnl, 'status':-2},conditions={'id': ('=', trade_id), 'symbol': symbol})
            if update_to_correct_details:
                buffer_print(f"✅ HEDGE trade {trade_id} details corrected:  [- Realized PnL: {realizedPnl} -] [- New Cummuative Closed PnL {newCumClosedPnl} -]")
                return
            else:
                buffer_print(f"⚠️ HEDGE trade: {trade_order_id} or {trade_id} has an issue updating to the correct trade details: [- Realized PnL: {realizedPnl} -] [- New Cummuative Closed PnL {newCumClosedPnl} -]")
                
def fetch_open_usdt_positions(exchange):
    try:
        response = exchange.fetch2('g-accounts/accountPositions', 'private', 'GET', {'currency': 'USDT'})
    except Exception:
        response = exchange.fetch2('accounts/positions', 'private', 'GET', {'currency': 'USDT'})

    all_positions = response.get('data', {}).get('positions', [])
    # Filter only open positions
    open_positions = [
        pos for pos in all_positions
        if float(pos.get('size', 0)) > 0
    ]
    return open_positions

# We try to find the matching market symbol that corresponds to this raw symbol
def find_matching_symbol(raw_symbol, markets):
    raw_symbol_clean = raw_symbol.replace(" ", "").upper()
    for market_symbol, market in markets.items():
        info_symbol = market['info'].get('symbol', '')
        if info_symbol.replace(" ", "").upper() == raw_symbol_clean:
            return market_symbol
    return None

def has_opposite_position_open(exchange, symbol, intended_side):
    """
    Works for hedge mode: checks if opposite posSide is already open.
    """
    try:
        positions = exchange.fetch_positions([symbol], params={'type': 'swap'})
        if not positions:
            return False
        
        for pos in positions:
            pos_side = pos.get('info', {}).get('posSide', '').lower()  # 'long' or 'short'
            size = float(pos.get('contracts') or pos.get('size', 0))
            
            if size > 0:
                if intended_side.lower() == 'buy' and pos_side.lower() == 'short':
                    return True
                elif intended_side.lower() == 'sell' and pos_side.lower() == 'long':
                    return True

        return False
    except Exception as e:
        print(f"Error checking hedge-mode positions: {e}")
        return False

    
        
def sync_open_orders_to_db(exchange, user_id):
    """
    Sync actual executed market orders (not pending limit/market) to DB for given user_id.
    """
    try:
        markets = exchange.load_markets()
        # Check if this order already exists in DB
        conn = db_conn.get_connection()
        cursor = conn.cursor()
        positions = fetch_open_usdt_positions(exchange)
        for position in positions:
            # print("Position: ", position)
            raw_pos_symbol = position['symbol']
            side = get_position_side(position)
            side_int = 0 if side == 'buy' else 1 if side == 'sell' else None
            symbol = find_matching_symbol(raw_pos_symbol, markets)
            if not symbol:
                print(f"No matching ccxt market symbol found for raw position symbol {raw_pos_symbol}")
            contracts = float(position.get('contracts', 0))
            matching_order_id = f"{user_id}_{symbol}_live_{side}"

            cursor.execute(
                "SELECT 1 FROM opn_trade WHERE symbol=%s AND trade_type=%s AND user_cred_id=%s AND status = 1 LIMIT 1",
                (symbol, side_int, user_id)
            )
            if cursor.fetchone():
                continue  # Already stored
            # print(f"Symbol: {symbol} and side: {side}")
            side_int = 0 if side == 'buy' else 1 if side == 'sell' else None
            
            trail_thresh = 0.20  # 10%
            profit_target_distance = 0.01  # 6%
            
            # Trade Data Payload
            strategy_type = "initial" if side == 'sell' else "hedge" if side == 'buy' else "Nopee"
            # 🔒 Default safe values
            hedged_res = 0
            parent_trade_id = 0
            if side == "sell":
                is_hedged = has_opposite_position_open(exchange, symbol, side)
                hedged_res = 1 if is_hedged else 0
                if is_hedged:
                    trail_thresh = 0.10  # 10%
                    profit_target_distance = 0.01  # 6%
            if side == "buy":
                parent_trade_id = GetParentTrade(db_conn, "opn_trade", user_id, symbol, 1)

            is_hedged = has_opposite_position_open(exchange, symbol, side)
            

            trade_data = {
                "strategy_type": strategy_type,
                "user_cred_id": user_id,
                "child_to": parent_trade_id,
                "trade_signal": -10,
                "order_id": matching_order_id,
                "symbol": symbol,
                "trade_type": side_int,
                "amount": float(position.get('size', 0)),
                "leverage": float(position.get('leverageRr', 1)),
                "trail_threshold": trail_thresh,
                "profit_target_distance": profit_target_distance,
                "cum_close_threshold": 1.0,
                "cum_close_distance": 0.5,
                "trade_done": 0,
                "re_entry_count": -1,
                "hedged": hedged_res,
                "hedge_start": 3,
                "status": 1
            }

            if insert_trade_data(db_conn, trade_data):
                print(f"✅ Inserted trade for {symbol} [order_id: {matching_order_id}]")
                backup_data = {
                    "token": TOKEN,
                    "user_id": user_id,
                    "order_id": matching_order_id,
                    "symbol": symbol,
                    "trade_type": side_int,
                    "amount": float(position.get('size', 0)),
                    "leverage": float(position.get('leverageRr', 1)),
                    "trail_threshold": trail_thresh,
                    "profit_target_distance": profit_target_distance,
                    "trade_done": 0,
                    "status": 1
                }

                if save_trade_history(backup_data):
                    print(f"☁️ Backup complete for {symbol} [order_id: {matching_order_id}]")
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"❌ Error syncing symbol {symbol}: {e}")

def GetOpenedTradeDetails(position, symbol, side):
    try:
        opened_position = [
            pos for pos in position
            if pos['symbol'] == symbol
            and (float(pos.get('info', {}).get('size', 0)) > 0 or pos.get('contracts', 0) > 0)
            and get_position_side(pos).lower() == side.lower()
        ]
        
        if not opened_position:
            print("⚠️ No matching position found.")
            return None, None

        pos = opened_position[0]  # ✅ Safely access the first (and should be only) matching item
        execSeq = pos.get('info', {}).get('execSeq')  # ✅ Access nested field safely
        cumClosedPnl = pos.get('info', {}).get('cumClosedPnlRv')  # ✅ Same here

        return execSeq, cumClosedPnl

    except Exception as e:
        print(f"❌ Error in GetopenedTradeDetails: {e}")
        return None, None
    
def GetClosedTradeDetails(position, symbol, side):
    try:
        closed_position = [
            pos for pos in position
            if pos['symbol'] == symbol
            and float(pos.get('info', {}).get('size', 0)) == 0 or float(pos.get('contracts', 0) == 0)
            and get_position_side(pos).lower() == side.lower()
        ]
        
        # print("Symbol :", side, "poss: ", position)
        
        if not closed_position:
            print("⚠️ No matching position found.")
            return None, None
        
        pos = closed_position[0]  # ✅ Safely access the first (and should be only) matching item

        execSeq = pos.get('info', {}).get('execSeq')  # ✅ Access nested field safely
        cumClosedPnl = pos.get('info', {}).get('cumClosedPnlRv')  # ✅ Same here

        return execSeq, cumClosedPnl

    except Exception as e:
        print(f"❌ Error in GetClosedTradeDetails: {e}")
        return None, None

    
def process_single_position(exchange, pos, signal_map, positionst):
    # print("pos-single-position: ", pos)
    symbol = pos['symbol']
    info = pos.get('info', {})
    
    # Determine actual position side
    if info.get('posMode') == 'Hedged':
        side_setup = info.get('posSide', '').lower()  # normalize
    else:
        side_setup = pos.get('side', '').lower()

    row = signal_map.get((symbol, side_setup), {})
    if not row:
        # buffer_print(f"⚠️ No matching trade signal for {symbol} with side '{side_setup}' (TradeMode: {info.get('posMode')})")
        return
    
    # print("roww: ", row)

    # Proceed as before
    # Default fallbacks if values are missing
    user_id = row.get('user_cred_id')
    strategy_type = row.get('strategy_type')
    trade_id = row.get('id')
    symbol_db = row.get('symbol')
    trade_order_id = row.get('order_id')
    trail_order_id = row.get('trail_order_id')
    trade_live_size = row.get('lv_size')
    trail_thresh = float(row.get('trail_threshold', 0.10))
    trail_profit_distance = float(row.get('profit_target_distance', 0.01))
    cum_close_threshold = float(row.get('cum_close_threshold', 1.0))
    cum_close_distance = float(row.get('cum_close_distance', 0.5))
    side_int = row.get('trade_type')
    trade_done = row.get('trade_done')
    trade_reentry_count_db = row.get('re_entry_count')
    trade_reentry_count = int(trade_reentry_count_db or 0)
    dn_allow_rentry = row.get('dn_allow_rentry')
    hedged = row.get('hedged')
    hedge_start = row.get('hedge_start')
    hedge_limit = row.get('hedge_limit')
    child_to = row.get('child_to')
    status = row.get('status')
    side = 'buy' if side_int == 0 else 'sell' if side_int == 1 else None
    side_ver = 'long' if side == 'buy' else 'short'
    execSeqValueDb = row.get('execSeq')
    prevcumClosedPnlDb = row.get('prevCumClosedPnl')
    
    allSymbolPosition = exchange.fetchPositions([symbol])
    
    try:
        if pos.get('contracts', 0) > 0:
            
            symbolPosition = [pos for pos in allSymbolPosition
                                if pos.get('symbol') == symbol
                                and (float(pos.get('info', {}).get('size', 0)) > 0 or pos.get('contracts', 0) > 0)]
            # Correction Logic
            execSeqValueFunc, cumClosedPnlFunc = GetOpenedTradeDetails(allSymbolPosition, symbol, side_ver)
            
            # print(f"Symbol: {symbol} - Strategy_type:  {strategy_type} - Hedge_limit: {hedge_limit} - Side: {side} - Hedged: {hedged}")
            trailing_stop_logic(exchange, pos, user_id, trade_id, trade_order_id,
            trail_order_id, trail_thresh, trail_profit_distance, 0.01, 0.1, cum_close_threshold, cum_close_distance, trade_reentry_count, hedged, symbolPosition)
            if strategy_type == "initial":
                monitor_position_and_reenter(exchange, user_id, trade_id, symbol, pos, trade_live_size, trade_reentry_count, dn_allow_rentry, hedged, hedge_start,hedge_limit, False)
                if hedge_limit == 1:
                    monitor_hedge_position(exchange, user_id, trade_id, symbol, hedged, status)
            if execSeqValueFunc is not None and (execSeqValueFunc != execSeqValueDb or execSeqValueDb == ''):
                update_execSeq = update_row(db_conn,table_name='opn_trade',updates={'execSeq': execSeqValueFunc},conditions={'id': ('=', trade_id), 'symbol': symbol})
                print(f"ExecSeq: {execSeqValueFunc}")
                if update_execSeq:
                    print(f"✅ UPDATED 'execSeqValueDb' to {execSeqValueFunc} for tradeId: {trade_id}")
            if cumClosedPnlFunc != prevcumClosedPnlDb:
                update_cumClosedPnl = update_row(db_conn,table_name='opn_trade',updates={'prevcumClosedPnl': cumClosedPnlFunc},conditions={'id': ('=', trade_id), 'symbol': symbol})
                # print(f"cumclosed value: {cumClosedPnlFunc}")
                if update_cumClosedPnl:
                    print(f"✅ UPDATED 'prevcumClosedPnlDb' to {cumClosedPnlFunc} for tradeId: {trade_id}")
        else:
            symbolPosition = [pos for pos in allSymbolPosition
                                if pos.get('symbol') == symbol
                                and (float(pos.get('info', {}).get('size', 0)) == 0 or pos.get('contracts', 0) == 0)]
            # Correction Logic
            execSeqValueFunc, cumClosedPnlFunc = GetClosedTradeDetails(allSymbolPosition, symbol, side_ver)
            # print(f"Found details: ExecSeq = {execSeqValueFunc} | CumClosedPnl= {cumClosedPnlFunc}")
            # print(f"SIDE: {side} -from- {side_int} |and| {execSeqValueDb}")
            mark_trade_signal_closed_if_position_closed(exchange, strategy_type, symbol, trade_order_id, trade_id, side, trade_reentry_count, execSeqValueDb, prevcumClosedPnlDb, symbolPosition)
        # buffer_print(f"--------------🙌 Position processed for {symbol} 🙌---------------")
        flush_symbol_buffer()
    except Exception as e:
        buffer_print(f"❌ Error processing position for symbol {symbol}: {e}")
        traceback.print_exc()


# def start_redis_listener_thread():
#     def runner():
#         loop = asyncio.new_event_loop()
#         asyncio.set_event_loop(loop)
#         loop.run_until_complete(redis_cache_listener())

#     threading.Thread(target=runner, daemon=True).start()
#     print("🚀 Redis cache listener thread started")
#     return True

# def start_db_publisher_thread():
#     def runner():
#         loop = asyncio.new_event_loop()
#         asyncio.set_event_loop(loop)
#         loop.run_until_complete(db_to_redis_publisher())

#     threading.Thread(target=runner, daemon=True).start()
#     print("🚀 DB-to-Redis publisher thread started")
#     return True

def map_symbol(exchange, db_symbol):
    for market in exchange.markets.values():
        if db_symbol.replace(':USDT', '') in market['id'] or db_symbol in market['symbol']:
            return market['symbol']
    return None

def main_job(exchange, user_cred_id, verify):
    try:    
        # trade_signals = fetch_trade_signals(user_cred_id=user_cred_id, status=1)
        trade_signals = fetch_trade_signals(user_cred_id=user_cred_id, status=1)
        # print("Tradesignal-cache: ", trade_signals)
        if not trade_signals:
            # buffer_print(f"[{exchange.apiKey[:6]}...] ⚠️ No trade signals found.")
            return

        signal_map = {
            (row['symbol'], 'long' if row.get('trade_type') == 0 else 'short'): row
            for row in trade_signals
            if row.get('status') == 1
        }
        # print(f"Signal Map: {signal_map}")
        markets = exchange.load_markets()
        def find_market_key(ccxt_sym, markets):
            if ccxt_sym in markets:
                return ccxt_sym
            for mk in markets:
                if mk.replace(' ', '') == ccxt_sym.replace(' ', ''):
                    return mk
            return None
        ccxt_to_raw = {
            (ccxt_sym, side): markets[find_market_key(ccxt_sym, markets)]['info']['symbol']
            for (ccxt_sym, side) in signal_map
            if find_market_key(ccxt_sym, markets) is not None
        }
        raw_to_ccxt = {(v, side): k for (k, side), v in ccxt_to_raw.items()}

        # raw_symbols = list(ccxt_to_raw.values())
        # print("rr: ", raw_symbols)
        
        # Extract raw symbols directly from DB
        raw_symbols = list({row['symbol'] for row in trade_signals})
        
        # positionst = exchange.fetch_positions(symbols=raw_symbols, params={'type': 'swap'})
        allPositions = exchange.fetchPositions(raw_symbols)
        # print("allposition: ", allPositions)
        positionst = [pos for pos in allPositions
                      if (float(pos.get('info', {}).get('size', 0)) > 0 or pos.get('contracts', 0) > 0)]
        usdt_balances = exchange.fetch_balance({'type': 'swap'}).get('USDT', {})
        # buffer_print(f"[{exchange.apiKey[:6]}...] USDT Balance->Free: {usdt_balances.get('free', 0)}")
        # buffer_print(f"[{exchange.apiKey[:6]}...] USDT Balance->Total: {usdt_balances.get('total', 0)}")

        for pos in allPositions:
            if stop_event.is_set():
                buffer_print(f"Stop event set, exiting main_job early for {exchange.apiKey[:6]}...")
                return
            symbol = pos['symbol']
            info = pos.get('info', {})
            # Determine actual position side
            if info.get('posMode') == 'Hedged':
                side = info.get('posSide')  # 'Long' or 'Short'
            else:
                side = pos.get('side')  # 'long' or 'short'
            thread_key = f"{exchange.apiKey}_{symbol}_{side}"

            # Use a lock per symbol per account
            with symbol_locks_lock:
                if thread_key not in symbol_locks:
                    symbol_locks[thread_key] = threading.Lock()

            def run_symbol_thread(pos=pos, symbol=symbol, thread_key=thread_key):
                lock = symbol_locks[thread_key]
                if lock.locked():
                    # buffer_print(f"🔁 Waiting for lock on {thread_key} - {symbol} — already being processed.")
                    return  # Thread already handling this symbol

                def locked_runner():
                    with lock:
                        try:
                            process_single_position(exchange, pos, signal_map, positionst)
                        except Exception as e:
                            buffer_print(f"❌ Error processing position for symbol {symbol}: {e}")
                            traceback.print_exc()
                        finally:
                            # buffer_print(f"✅ Thread cleanup done for {symbol}")
                            pass

                threading.Thread(target=locked_runner, daemon=False).start()

            run_symbol_thread()
            time.sleep(0.8)  # small throttle
    except Exception as e:
        buffer_print(f"❌ Error in main_job for [{exchange.apiKey[:6]}...]: {e}")
        traceback.print_exc()
        
account_locks = {}
def run_exchanges_in_batch(batch):
    while not stop_event.is_set():
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(batch)) as executor:
                futures = []
                for item in batch:
                    if len(item) != 3:
                        buffer_print(f"⚠️ Unexpected tuple size: {item}")
                        continue

                    exchange_obj, user_cred_id, verify = item

                    if user_cred_id not in account_locks:
                        account_locks[user_cred_id] = Lock()

                    def locked_main_job(exchange_obj=exchange_obj, user_cred_id=user_cred_id, verify=verify):
                        if stop_event.is_set():
                            return
                        with account_locks[user_cred_id]:
                            if not stop_event.is_set():
                                main_job(exchange_obj, user_cred_id, verify)

                    futures.append(executor.submit(locked_main_job))

                for future in concurrent.futures.as_completed(futures):
                    if stop_event.is_set():
                        break
                    try:
                        result = future.result()
                        # print(f"Task completed with result: {result}")
                    except Exception as e:
                        buffer_print(f"❌ Error in batch task: {e}")
                        traceback.print_exc()

        except KeyboardInterrupt:
            buffer_print("🛑 KeyboardInterrupt received in run_exchanges_in_batch, stopping...")
            stop_event.set()
            break
        except Exception as e:
            buffer_print(f"❌ Unexpected exception in batch: {e}")
            traceback.print_exc()

        # print("Batch iteration complete, sleeping 0.8s")
        if not stop_event.is_set():
            time.sleep(1)



def sync_open_orders_loop_batch(batch):
    cooldown_seconds = 1 * 60  # 30 minutes
    last_sync_times = { (ex.id, user_id): 0 for ex, user_id, _ in batch }

    while not stop_event.is_set():
        current_time = time.time()
        for exchange, user_id, _ in batch:
            key = (exchange.id, user_id)
            if current_time - last_sync_times[key] >= cooldown_seconds:
                try:
                    sync_open_orders_to_db(exchange, user_id)
                    last_sync_times[key] = current_time
                except Exception as e:
                    print(f"❌ Error syncing user {user_id} on {exchange.id}: {e}")
        # Sleep a bit to avoid tight loop
        time.sleep(min(2, cooldown_seconds / 2))  # e.g. 2 seconds


active_cred_ids = set()
exchange_list = []  # global list of (exchange, cred_id, verify) tuples
def monitor_new_credentials():
    global exchange_list
    while not stop_event.is_set():
        try:
            credentials = get_all_credentials_with_exchange_info(db_conn)
            # print("Cred: ", credentials)
            new_accounts = []
            
            for row in credentials:
                cred_id = row['cred_id']
                if cred_id in active_cred_ids:
                    continue
                
                try:
                    # print("Checking cred_id:", cred_id, "Already active:", cred_id in active_cred_ids)
                    exchange_name = row['exchange_name']
                    requires_password = row['requirePass']
                    password = row['password'] if requires_password != 0 else None
                    verify = row['api_key'][:6]
                    print(f"🔧 Creating exchange for [{verify}...]")
                    exchange = create_exchange(exchange_name, row['api_key'], row['secret'], password)
                    print(f"🔧 Creating exchange for [{exchange}...]")
                    exchange.options['warnOnFetchOpenOrdersWithoutSymbol'] = False

                    active_cred_ids.add(cred_id)
                    new_accounts.append((exchange, cred_id, verify))
                    buffer_print(f"🟢 Detected new account [{verify}...]")
                except Exception as e:
                    buffer_print(f"❌ Failed to initialize new exchange [{row['api_key'][:6]}...]: {e}")

            if new_accounts:
                exchange_list.extend(new_accounts)
                print("New Account added to 'account list'")

            # You can optionally detect removed accounts here and remove them from active_cred_ids & exchange_list

        except Exception as e:
            buffer_print(f"❌ Error during dynamic exchange scan: {e}")
            
        # Check stop_event here to allow early exit
        if stop_event.is_set():
            buffer_print("🛑 monitor_new_credentials stopping as stop_event is set.")
            break

        stop_event.wait(timeout=1)

def run_all():
    try:
        buffer_print("🚀 Bot started. Watching for new exchanges...")
        # Start monitor thread
        monitor_thread = threading.Thread(target=monitor_new_credentials, daemon=False)
        monitor_thread.start()
        while True:
            if stop_event.is_set():
                break
            total = len(exchange_list)
            if total == 0:
                time.sleep(1)
                continue

            batch_size = max(1, int(math.sqrt(total)))  # your batch logic
            batches = [exchange_list[i:i + batch_size] for i in range(0, total, batch_size)]

            # Dynamic max_workers based on total accounts (limit max to avoid overload)
            max_workers = min(total * 2, 100)  # max 100 threads as safety cap

            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []

                try:
                    # Submit batch jobs
                    for batch in batches:
                        print("Gotten here")
                        futures.append(executor.submit(run_exchanges_in_batch, batch))
                        futures.append(executor.submit(sync_open_orders_loop_batch, batch))

                    while not stop_event.is_set():
                        done, not_done = concurrent.futures.wait(futures, timeout=1)
                        # Optionally check status or requeue finished ones

                except KeyboardInterrupt:
                    print("🛑 Ctrl+C caught. Setting stop_event...")
                    stop_event.set()

                    # Attempt to cancel remaining futures (if they haven't started yet)
                    for future in futures:
                        future.cancel()

                finally:
                    print("🔄 Waiting for tasks to exit...")
                    concurrent.futures.wait(futures)  # Block until all exit
                    print("✅ All batch jobs stopped.")

                # Futures still running will be stopped in next loop iteration via stop_event if set

            time.sleep(1)  # slight pause before recomputing batches and re-submitting  
    except KeyboardInterrupt:
        buffer_print("\n🛑 Ctrl+C detected in run_all. Setting stop_event...")
        stop_event.set()

if __name__ == "__main__":
    try:
        # Initialize trade_signal_cache at startup
        sys.stdout.reconfigure(line_buffering=True)  # Prevent console buffer freezing
        clear_trade_signal_cache()
        # if start_redis_listener_thread():
        #     print("🚀 Redis live cache listener thread started")
        # if start_db_publisher_thread():
        #     print("🚀 Redis Start publisher thread started")
        run_all()
        
    except KeyboardInterrupt:
        buffer_print("\n🛑 Ctrl+C detected in main. Stopping all threads...")
        stop_event.set()
        time.sleep(2)
    finally:
        # Clean trade_signal_cache on exit
        clear_trade_signal_cache()
        
        buffer_print("🔚 Program exited cleanly.")
        os._exit(0)  # Force-exit all threads if still hanging