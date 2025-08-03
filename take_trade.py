import os
import sys
import io
import ccxt
import pandas as pd
import ta
import requests
import threading
import time
import math
import json
import schedule
import traceback

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

print_lock = threading.Lock()
stop_event = threading.Event()  # Global stop signal

# Trading parameters
leverage = -10
multiplier= 1.5
fromPercnt = 0.2  #20%

def create_exchange(exchange_name, api_key, secret, password=None):
    """Create and return a CCXT exchange instance"""
    try:
        exchange_class = getattr(ccxt, exchange_name)
        config = {
            'apiKey': api_key,
            'secret': secret,
            'enableRateLimit': True
        }
        
        # Add password if provided
        if password:
            config['password'] = password
            
        return exchange_class(config)
    except AttributeError:
        raise ValueError(f"Exchange '{exchange_name}' not found in CCXT")
    except Exception as e:
        raise Exception(f"Failed to create {exchange_name} exchange: {e}")
    
def save_trade_history(data: dict):
    try:
        response = requests.post(API_URL, json=data)
        print("🔍 RAW RESPONSE:", response.status_code)
        print("🔍 RESPONSE TEXT:", response.text)
        if response.status_code == 200:
            print("✅ Saved:", response.json())
        else:
            print("❌ Failed to save:", response.status_code, response.text)
    except Exception as e:
        print("⚠️ Error:", str(e))


# def thread_safe_print(*args, **kwargs):
#     with print_lock:
#         print(*args, **kwargs)

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

def has_open_position(exchange, symbol):
    try:
        positions = exchange.fetch_positions([symbol])  # Only for the given symbol
        for pos in positions:
            if pos['symbol'] == symbol and float(pos.get('contracts', 0)) > 0:
                return True
        return False
    except Exception as e:
        print(f"❌ Error checking live position for {symbol}: {e}")
        return False

def generate_mixed_token(exchange_name: str, secret: str, length: int = 24):
    combined = ""
    min_len = min(len(exchange_name), len(secret))

    # Interleave exchange_name and secret characters
    for i in range(min_len):
        combined += exchange_name[i] + secret[i]

    # If we still need more characters
    combined += secret[min_len:min_len + (length - len(combined))]

    return combined[:length]

# Helper function to get open long/short counts
def get_open_position_counts(exchange, all_symbols):
    positions = exchange.fetch_positions(symbols=all_symbols)
    open_positions = [pos for pos in positions if pos.get('contracts') and abs(float(pos['contracts'])) > 0]
    short_positions = [
        pos for pos in open_positions
        if (pos.get('side') == 'short') or
        ('size' in pos and float(pos['size']) < 0) or
        ('info' in pos and pos['info'].get('side', '').lower() == 'sell')
    ]
    long_positions = [
        pos for pos in open_positions
        if (pos.get('side') == 'long') or
        ('size' in pos and float(pos['size']) > 0) or
        ('info' in pos and pos['info'].get('side', '').lower() == 'buy')
    ]
    return open_positions, len(short_positions), len(long_positions)

def issueNumberOfTrade(acc_bal):
    thresholds = [
        (10000, 20),
        (5000, 20),
        (1000, 20),
        (500, 20),
        (150, 20),
        (0, 20)
    ]
    
    for limit, trades in thresholds:
        if acc_bal >= limit:
            return trades
        
# def calculateIntialAmount(account_balance, leverage= 5, divider= 5.0):
#     MAX_NUMBER_TRADE = issueNumberOfTrade(account_balance)
#     FIRST_ENTRY = round_to_sig_figs((account_balance / divider), 2)
#     FIRST_ENTRY_PER_TRADE = round_to_sig_figs((FIRST_ENTRY / MAX_NUMBER_TRADE), 2)
#     INITIAL_AMOUNT = FIRST_ENTRY_PER_TRADE * leverage
#     return FIRST_ENTRY_PER_TRADE

def calculateIntialAmount(account_balance, leverage= 10, divider= 12.5):
    MAX_NUMBER_TRADE = issueNumberOfTrade(account_balance)
    FIRST_ENTRY = round_to_sig_figs((account_balance / divider), 2)
    FIRST_ENTRY_PER_TRADE = round_to_sig_figs((FIRST_ENTRY / MAX_NUMBER_TRADE), 2)
    INITIAL_AMOUNT = FIRST_ENTRY_PER_TRADE * abs(leverage)
    # print(f"{account_balance} -- FIRST_ENTRY {FIRST_ENTRY} -- FIRST_ENTRY_PER_TRADE {FIRST_ENTRY_PER_TRADE} -- INITIAL_AMOUNT {INITIAL_AMOUNT}")
    return INITIAL_AMOUNT

def check_equity_usage(balance):
    total = float(balance.get('total', 0))
    free = float(balance.get('free', 0))
    used = total - free
    return total > 0 and used >= 0.7 * total

def calculateLiquidationTargPrice(_liqprice, _entryprice, _percnt, _round):
    return round_to_sig_figs(_entryprice + (_liqprice - _entryprice) * _percnt, _round)

def get_base_amount(exchange, symbol, usdt_value):
    ticker = exchange.fetch_ticker(symbol)
    market_price = ticker['last']
    base_amount = usdt_value / market_price
    return base_amount

# MAIN LOOP
def main_job(exchange, user_cred_id, token, verify):
    try:
        # drop_table("opn_trade")
        if stop_event.is_set():
            return

        signal = fetch_single_trade_signal(db_conn)
        if not signal:
            # thread_safe_print("ℹ️ No active signals available.")
            return
        
        trade_signal_id = signal['id']
        symbol = signal['symbol_pair']
        side = signal['trade_type']
        trail_thresh = 0.20 # 10% default
        profit_target_distance = 0.01 # 60% default

        usdt_balances = exchange.fetch_balance({'type': 'swap'}).get('USDT', {})
        usdt_balance_free = usdt_balances.get('free', 0)
        usdt_balance_total = usdt_balances.get('total', 0)
        
        if check_equity_usage(usdt_balances):
            return
        
        time.sleep(2)
        MAX_NO_SELL_TRADE = issueNumberOfTrade(usdt_balance_total)
        MAX_NO_BUY_TRADE = 2


        position_count = get_side_count(db_conn, user_cred_id, 0, side)
        # print("Position Count: ", position_count)
        
        if (side == 0 and position_count >= MAX_NO_BUY_TRADE):
            pass
            # print(f"❌ Max number of buy trades reached ({position_count})!")
        elif (side == 1 and position_count >= MAX_NO_SELL_TRADE):
            pass
            # print(f"❌ Max number of sell trades reached ({position_count})!")
        else:
            if has_open_trade(db_conn, user_cred_id, symbol):
                # print(f"⚠️ Trade {symbol} already taken for {user_cred_id}")
                return

            if has_open_position(exchange, symbol):
                # print(f"⚠️ Skipping {symbol} — already has an open position on exchange")
                return

            # ✅ Proceed with trade execution
            # thread_safe_print(f"✅ {verify if hasattr(exchange, 'id') else 'Exchange'} → Processing signal: {signal}")
            
            side_n_str = "buy" if side == 0 else "sell"
            usdt_amount = calculateIntialAmount(usdt_balance_total, leverage)
            
            base_amount = get_base_amount(exchange, symbol, usdt_amount)
            
            market_order_id = place_entry_and_liquidation_limit_order(
                exchange, symbol, side_n_str, base_amount, leverage
            )
            
            # REDIS ORT -- 6379
            if market_order_id:
                take_trade_data = {
                    "strategy_type": "initial",
                    "user_cred_id": user_cred_id,
                    "child_to": 0,
                    "trade_signal": trade_signal_id,
                    "order_id": market_order_id,
                    "symbol": symbol,
                    "trade_type": side,
                    "amount": base_amount,
                    "leverage": leverage,
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
                
                if insert_trade_data(db_conn, take_trade_data):
                    print("📤 Sending backup trade data...")
                    backup_trade_data = {
                        "token": TOKEN,
                        "user_id": user_cred_id,
                        "order_id": market_order_id,
                        "symbol": symbol,
                        "trade_type": side,
                        "amount": base_amount,
                        "leverage": leverage,
                        "trail_threshold": trail_thresh,
                        "profit_target_distance": profit_target_distance,
                        "trade_done": 0,
                        "status": 1
                    }
                    
                    save_trade_history(backup_trade_data)
                        
    except Exception as e:
        thread_safe_print(f"Main job error: {e}")
        traceback.print_exc()

def run_exchanges_in_batch(batch):
    index = 0
    while not stop_event.is_set():
        try:
            exchange, user_cred_id, token, verify = batch[index % len(batch)]
            main_job(exchange, user_cred_id, token, verify)  # Takes one random prioritized signal
            index += 1
            # time.sleep(0.4)  # Delay between each exchange in this thread's batch
        except Exception:
            thread_safe_print("⚠️ Error in batch round-robin. Retrying...")
            traceback.print_exc()
            time.sleep(5)


def run_all():
    credentials = get_all_credentials_with_exchange_info(db_conn)  # JOINed data
    if not credentials:
        thread_safe_print("⚠️ No API credentials found in the database. Exiting...")
        return

    exchange_list = []
    for row in credentials:
        try:
            exchange_name = row['exchange_name']
            requires_password = row['requirePass']
            password = row['password'] if requires_password != 0 else None
            verify = row['api_key'][:6]
            exchange = create_exchange(exchange_name, row['api_key'], row['secret'], password)
            
            # token = generate_mixed_token(exchange_name, row['secret'], length=30)
            exchange_list.append((exchange, row['cred_id'], TOKEN, verify))
        except Exception as e:
            print(row)
            thread_safe_print(f"❌ Failed to create exchange for API key {row['api_key'][:6]}...: {e}")

    if not exchange_list:
        thread_safe_print("⚠️ No valid exchanges could be created. Exiting...")
        return

    total = len(exchange_list)
    batch_size = max(1, int(math.sqrt(total)))  # √N batching for load balancing
    thread_safe_print(f"Total exchanges: {total}, Batch size: {batch_size}")

    batches = [exchange_list[i:i + batch_size] for i in range(0, total, batch_size)]

    threads = []
    for batch in batches:
        t = threading.Thread(target=run_exchanges_in_batch, args=(batch,), daemon=True)
        t.start()
        threads.append(t)

    try:
        while not stop_event.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        thread_safe_print("\n🛑 Ctrl+C detected, stopping all threads...")
        stop_event.set()

if __name__ == "__main__":
    run_all()