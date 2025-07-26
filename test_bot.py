import ccxt
import json

# Replace with your actual API credentials
api_key = "c895dd8c-1cc1-4e88-86a2-b6e856c274ba"
secret = "qO73E8E6SsLlQ4TDgjhGpe-h7KWZYYhdNHl-bYLW0woyOTNiNWFlYi1lYmJmLTRmOWEtYTIxOS1hY2RkMjk3YzM5MjI"

exchange = ccxt.phemex({
  'apiKey': api_key,
  'secret': secret,
  'enableRateLimit': True,
  'options': {
      'type': 'swap',
      'defaultType': 'swap',
  },
})

# exchange = ccxt.binance()


# import time
# from datetime import datetime

# # 🔐 Add the endpoint definition so CCXT knows how to sign it
# exchange.api['private']['get']['exchange/order/trade'] = 5

# def fetch_closed_positions(exchange, symbol, limit=20):
#     """
#     Fetch recent closed trades for a specific symbol using Phemex's 'exchange/order/trade' endpoint.

#     :param exchange: ccxt phemex exchange instance
#     :param symbol: str, market symbol like "DOGE/USDT:USDT"
#     :param limit: int, number of recent trades to fetch
#     :return: list of dicts with trade info
#     """
#     try:
#         # Attempt to fetch trades
#         response = exchange.fetch2(
#             'exchange/order/trade',
#             'private',
#             'GET',
#             {'symbol': symbol}
#         )

#         data = response.get('data', [])
#         trades = data if isinstance(data, list) else [data]

#         result = []
#         for t in trades:
#             info = t.get('info', {})
#             side = 'buy' if info.get('side') == '1' else 'sell' if info.get('side') == '2' else t.get('side', 'unknown')
#             amount = float(info.get('execQtyRq', t.get('amount', 0)))
#             price = float(info.get('execPriceRp', t.get('price', 0)))
#             ts_ns = int(info.get('createdAt') or t.get('timestamp', 0))
#             ts = datetime.utcfromtimestamp(ts_ns / 1000) if ts_ns > 1e12 else datetime.utcnow()

#             print(f"Symbol: {symbol}, Side: {side}, Amount: {amount}, Price: {price}, Time: {ts.isoformat()}")

#             result.append({
#                 'symbol': symbol,
#                 'side': side,
#                 'amount': amount,
#                 'price': price,
#                 'timestamp': ts,
#                 'info': info
#             })

#         return result

#     except Exception as e:
#         print(f"Error fetching closed positions: {e}")
#         return []


# def fetch_closed_positions(exchange, symbol, limit=20):
#     try:
#         # Try both possible endpoints
#         try:
#             response = exchange.fetch2('g-accounts/accountPositions', 'private', 'GET', {'currency': 'USDT'})
#         except Exception:
#             response = exchange.fetch2('accounts/positions', 'private', 'GET', {'currency': 'USDT'})
        
#         all_positions = response.get('data', {}).get('positions', [])

#         # Filter only closed positions in time window
#         closed_positions = [
#             pos for pos in all_positions
#             if float(pos.get('size', 0)) > 0
#         ]

#         return closed_positions

#     except Exception as e:
#         print(f"Error fetching closed positions: {e}")
#         return []


# List all callable methods of the exchange
# all_methods = [method for method in dir(exchange) if callable(getattr(exchange, method)) and not method.startswith('_')]

# for method in sorted(all_methods):
#     print(method)

# trades = exchange.fetchTrades("A/USDT:USDT")

# print(f"{trades}")
# print(json.dumps(exchange.api, indent = 4))
# print(fetch_closed_positions(exchange))
# closed_last_hour = fetch_closed_positions(exchange, "BUSDT")
# for p in closed_last_hour:
#     print(f"{p['symbol']} closed with PnL: {p['cumClosedPnlRv']}")
#     print(f"\n\t\t{p}\n")

# closed_trades = fetch_closed_positions(exchange, "DOGE/USDT:USDT", limit=10)
# print(closed_trades)

# exchange.verbose = True 
# response = exchange.request(
#     "exchange/order/trade?symbol=BTCUSDT&limit=10",
#     "private",
#     "GET",
#     {}
# )
# print(response)

# Check available methods
# print("Available methods:")
# print(f"fetchPositions: {exchange.has['fetchPositions']}")
# print(f"fetchPositionsHistory: {exchange.has['fetchPositionsHistory']}")
# print(f"fetchMyTrades: {exchange.has['fetchMyTrades']}")

# # Print all available private methods
# private_methods = [method for method in dir(exchange) if 'private' in method.lower()]
# print("Private methods:", json.dumps(private_methods, indent=2))
# Check futures trades
# Replace 'BTCUSDT' with your actual symbol
# Since your bot is trading, let's see what positions you have

# from datetime import datetime, timedelta

# def convert_timestamp(timestamp):
#     """Auto-detect timestamp format and convert to date"""
#     if not timestamp:
#         return "N/A"
    
#     if isinstance(timestamp, str):
#         timestamp = int(timestamp)
    
#     # Determine format by number of digits
#     if timestamp > 10**15:  # nanoseconds
#         timestamp = timestamp / 1_000_000_000
#     elif timestamp > 10**12:  # microseconds  
#         timestamp = timestamp / 1_000_000
#     elif timestamp > 10**10:  # milliseconds
#         timestamp = timestamp / 1000
#     # else: seconds (no conversion needed)
    
#     dt = datetime.fromtimestamp(timestamp)
#     return dt.strftime("%Y-%m-%d %H:%M:%S")

# def find_closing_position(open_execSeq, side, symbol, closed_positions):
#     candidates = [
#         p for p in closed_positions 
#         if p['symbol'] == symbol 
#         and p['side'].lower() == side.lower()
#         and int(p['execSeq']) > int(open_execSeq)
#     ]
#     # Sort by execSeq to get the immediate next
#     candidates.sort(key=lambda p: int(p['execSeq']))
#     return candidates[0] if candidates else None

# positions = exchange.fetchPositions(["RSR/USDT:USDT"])
# for pos in positions:
#     if pos['contracts'] == 0:  # active positions
#       print(pos)
      # print(f"Active position symbol: {pos['symbol']}")
      # print(f"Cummulative Closed PNL: {pos['info']['cumClosedPnlRv']}")
      # print(f"Entry time: {convert_timestamp(pos['info']['transactTimeNs'])}")
      # print(f"Percentage: {pos['percentage']}")

def find_closing_position(open_execSeq, side, symbol, closed_positions):
    open_execSeq = int(open_execSeq)
    valid_candidates = []

    for p in closed_positions:
        try:
            pos_execSeq = int(p['info']['execSeq'])
            pos_size = float(p['info'].get('size', '0'))

            if (
                p['symbol'] == symbol
                and p.get('side', '').lower() == side.lower()
                and pos_size == 0
                and pos_execSeq > open_execSeq
            ):
                valid_candidates.append((pos_execSeq, p))
        except (KeyError, ValueError, TypeError):
            continue  # skip invalid entries

    if not valid_candidates:
        return None

    # Sort by smallest difference from open_execSeq
    closest = min(valid_candidates, key=lambda x: x[0] - open_execSeq)
    return closest[1]

# Example input (from saved open position)
saved_open_execSeq = "1089315203"
saved_open_side = "long"
saved_open_symbol = "SPK/USDT:USDT"

cumClosedPnlRvOld = 0.04249  # 0.04235

# Fetch latest positions
positions = exchange.fetchPositions([saved_open_symbol])
closed_positions = [p for p in positions if p['contracts'] == 0]

print(closed_positions)

# # Find the correct closed match
# matched_closed = find_closing_position(saved_open_execSeq, saved_open_side, saved_open_symbol, closed_positions)

# # Output
# if matched_closed:
#     print("Matched Closed Position:")
#     print("execSeq:", matched_closed['info']['execSeq'])
#     print("cumClosedPnlRv:", matched_closed['info']['cumClosedPnlRv'])
#     profit = float(matched_closed['info']['cumClosedPnlRv']) - cumClosedPnlRvOld
#     print(f"Profit: {profit:.4f}")
# else:
#     print("No matching closed position found.")




# Opened position data below

# {'info': {'userID': '4736653', 'accountID': '47366530003', 'symbol': 'RSRUSDT', 'currency': 'USDT', 'side': 'Sell', 'positionStatus': 'Normal', 'crossMargin': False, 'leverageRr': '5', 'initMarginReqRr': '0.2', 'maintMarginReqRr': '0.01', 'riskLimitRv': '162500', 'size': '14', 'valueRv': '0.132244', 'avgEntryPriceRp': '0.009446', 'avgEntryPrice': '0.009446', 'posCostRv': '0.0258386856', 'assignedPosBalanceRv': '0.02662801568', 'bankruptCommRv': '0.00007801038', 'bankruptPriceRp': '0.011341', 'positionMarginRv': '0.0265500053', 'liquidationPriceRp': '0.011228', 'deleveragePercentileRr': '0', 'buyValueToCostRr': '0.20108', 'sellValueToCostRr': '0.20132', 'markPriceRp': '0.009245', 'estimatedOrdLossRv': '0', 'usedBalanceRv': '0.05727053024', 'cumClosedPnlRv': '-0.00693', 'cumFundingFeeRv': '-0.0000132104', 'cumTransactFeeRv': '0.0002473584', 'transactTimeNs': '1752768000023307780', 'takerFeeRateRr': '-1', 'makerFeeRateRr': '-1', 'term': '2', 'lastTermEndTimeNs': '1752758115806168458', 'lastFundingTimeNs': '1752768000000000000', 'curTermRealisedPnlRv': '-0.000066136', 'execSeq': '49894111602', 'posSide': 'Merged', 'posMode': 'OneWay', 'buyLeavesValueRv': '0', 'buyLeavesQtyRq': '0', 'sellLeavesValueRv': '0.152208', 'sellLeavesQtyRq': '14'}, 'id': None, 'symbol': 'RSR/USDT:USDT', 'contracts': 14.0, 'contractSize': 1.0, 'unrealizedPnl': 32.22324225591552, 'leverage': 5.0, 'liquidationPrice': 0.011228, 'collateral': 0.0265500053, 'notional': 0.132244, 'markPrice': 0.009245, 'lastPrice': None, 'entryPrice': 0.009446, 'timestamp': None, 'lastUpdateTimestamp': None, 'initialMargin': 0.02662801568, 'initialMarginPercentage': 0.2013551894982003, 'maintenanceMargin': 0.00132244, 'maintenanceMarginPercentage': 0.01, 'marginRatio': 0.04980940625273623, 'datetime': None, 'marginMode': 'isolated', 'side': 'short', 'hedged': False, 'percentage': None, 'stopLossPrice': None, 'takeProfitPrice': None}

# Closed position data below

# {'info': {'userID': '4736653', 'accountID': '47366530003', 'symbol': 'RSRUSDT', 'currency': 'USDT', 'side': 'None', 'positionStatus': 'Normal', 'crossMargin': False, 'leverageRr': '5', 'initMarginReqRr': '0.2', 'maintMarginReqRr': '0.01', 'riskLimitRv': '162500', 'size': '0', 'valueRv': '0', 'avgEntryPriceRp': '0', 'avgEntryPrice': '0', 'posCostRv': '0', 'assignedPosBalanceRv': '0', 'bankruptCommRv': '0', 'bankruptPriceRp': '0', 'positionMarginRv': '0', 'liquidationPriceRp': '0', 'deleveragePercentileRr': '0', 'buyValueToCostRr': '0.20108', 'sellValueToCostRr': '0.20132', 'markPriceRp': '0.00924', 'estimatedOrdLossRv': '0', 'usedBalanceRv': '0', 'cumClosedPnlRv': '-0.004046', 'cumFundingFeeRv': '-0.0000132104', 'cumTransactFeeRv': '0.0003249744', 'transactTimeNs': '1752778088086904388', 'takerFeeRateRr': '-1', 'makerFeeRateRr': '-1', 'term': '3', 'lastTermEndTimeNs': '1752778084160616522', 'lastFundingTimeNs': '1752768000000000000', 'curTermRealisedPnlRv': '0.002740248', 'execSeq': '49899096493', 'posSide': 'Merged', 'posMode': 'OneWay', 'buyLeavesValueRv': '0', 'buyLeavesQtyRq': '0', 'sellLeavesValueRv': '0', 'sellLeavesQtyRq': '0'}, 'id': None, 'symbol': 'RSR/USDT:USDT', 'contracts': 0.0, 'contractSize': 1.0, 'unrealizedPnl': None, 'leverage': 5.0, 'liquidationPrice': 0.0, 'collateral': 0.0, 'notional': 0.0, 'markPrice': 0.00924, 'lastPrice': None, 'entryPrice': 0.0, 'timestamp': None, 'lastUpdateTimestamp': None, 'initialMargin': 0.0, 'initialMarginPercentage': None, 'maintenanceMargin': 0.0, 'maintenanceMarginPercentage': 0.01, 'marginRatio': None, 'datetime': None, 'marginMode': 'isolated', 'side': 'short', 'hedged': False, 'percentage': None, 'stopLossPrice': None, 'takeProfitPrice': None}

