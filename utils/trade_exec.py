import threading #FIX THISSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSS
import ccxt
import math
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

def place_entry_and_liquidation_limit_order (exchange, symbol, side, amount, leverage):
    try:
        # 🟡 Fetch current market price
        ticker = exchange.fetch_ticker(symbol)
        market_price = ticker['last']
        # base_amount = usdt_value / market_price
        pos_side = 'Long' if side == 'buy' else 'Short'

        # 🟡 Set isolated margin
        # try:
        #     exchange.set_margin_mode('isolated', symbol)
        # except Exception as e:
        #     thread_safe_print(f"⚠️ Could not set isolated margin for {symbol}: {e}")
        #     try:
        #         exchange.set_margin_mode('isolated', symbol, params={'posSide': pos_side})
        #         thread_safe_print(f"Successfully set margin mode with posSide={pos_side}")
        #     except Exception as e2:
        #         thread_safe_print(f"Failed again with posSide: {e2}")

        # 🟡 Set leverage
        try:
            exchange.set_leverage(leverage, symbol)
        except Exception as e:
            thread_safe_print(f"⚠️ Could not set leverage for {symbol}: {e}")
            try:
                exchange.set_leverage(leverage, symbol, params={'posSide': pos_side})
                thread_safe_print(f"Successfully set leverage with posSide={pos_side}")
            except Exception as e2:
                thread_safe_print(f"Failed again with posSide: {e2}")
        
        ensure_hedge_mode(exchange, symbol)

        thread_safe_print(f"🔔 ORDER → {symbol} | {side.upper()} | Price: {market_price:.4f} | Qty: {amount:.5f}")

        # 🟢 Place market order (with fallback to posSide)
        try:
            market_order = exchange.create_order(
                symbol=symbol,
                type='market',
                side=side,
                amount=amount,
                params={'reduceOnly': False}
            )
        except ccxt.BaseError as e:
            if 'TE_ERR_INCONSISTENT_POS_MODE' in str(e):
                pos_side = 'Long' if side == 'buy' else 'Short'
                market_order = exchange.create_order(
                    symbol=symbol,
                    type='market',
                    side=side,
                    amount=amount,
                    params={
                        'reduceOnly': False,
                        'posSide': pos_side
                    }
                )
            else:
                raise

        thread_safe_print(f"✅ Market Order Placed: {market_order}")
        market_order_id = market_order.get('id')

        '''
        # 🟡 Fetch position and liquidation info
        positions = exchange.fetch_positions([symbol])
        pos_side_str = 'long' if side == 'buy' else 'short'
        position = next((p for p in positions if p['side'] == pos_side_str), None)

        if not position or not float(position.get('liquidationPrice') or 0):
            thread_safe_print("⚠️ No valid liquidation price found. Skipping limit order.")
            return market_order_id, None

        # Extract necessary fields
        liquidation_price = float(position.get('liquidationPrice'))
        entry_price = float(position.get('entryPrice') or 0)
        mark_price = float(position.get('markPrice') or 0)
        contracts = float(position.get('contracts') or 0)
        notional = float(position.get('notional') or 0)
        leverage = float(position.get('leverage') or 1)

        # 🧮 Determine precision
        price_precision = exchange.markets[symbol]['precision']['price']
        price_sig_digits = count_sig_digits(price_precision)
        amount_precision = exchange.markets[symbol]['precision']['amount']
        amount_sig_digits = count_sig_digits(amount_precision)

        thread_safe_print(f"📏 Price Precision: {price_precision}, Sig Digs: {price_sig_digits}")

        # 🔁 Calculate amount for limit order (2x notional size)
        double_notional = (notional / leverage) * multiplier
        order_amount = double_notional / mark_price
        order_amount = round_to_sig_figs(order_amount, amount_sig_digits)

        # 🎯 Calculate re-entry target price
        try:
            thread_safe_print("📐 Calculating re-entry target price...")
            target_price = calculateLiquidationTargPrice(entry_price, liquidation_price, fromPercnt, price_sig_digits)
            thread_safe_print(f"🎯 Target Price: {target_price}")
        except Exception as e:
            thread_safe_print(f"❌ Error calculating target price: {e}")
            return market_order_id, None

        # 🟢 Place limit order (with fallback to posSide)
        try:
            limit_order = exchange.create_order(
                symbol=symbol,
                type='limit',
                side=side,
                amount=contracts,
                price=target_price
            )
        except ccxt.BaseError as e:
            if 'TE_ERR_INCONSISTENT_POS_MODE' in str(e):
                limit_order = exchange.create_order(
                    symbol=symbol,
                    type='limit',
                    side=side,
                    amount=contracts,
                    price=target_price,
                    params={'posSide': pos_side_str.capitalize()}
                )
            else:
                raise

        thread_safe_print(f"📌 Limit Order Placed: {limit_order}")
        limit_order_id = limit_order.get('id')
        '''
        return market_order_id

    except Exception as e:
        thread_safe_print(f"❌ Unexpected error for {symbol}: {e}")
        return None
    
    
def place_limit_buy_above_market(exchange, symbol, side, type, amount, pip_count=15):
    try:
        # Load markets if not already loaded
        markets = exchange.load_markets()
        market = markets[symbol]
        
        # Get tick size (minimal price movement)
        pip_size = market['precision']['price']
        pip_size_count = count_sig_digits(pip_size)
        if not pip_size:
            raise ValueError(f"⚠️ Could not determine pip size for symbol {symbol}")

        # Get current market price (prefer mark price if available)
        ticker = exchange.fetch_ticker(symbol)
        current_price = ticker.get('mark', ticker['last'])

        # Calculate limit price: current + (pip_size * pip_count)
        limit_price = round(current_price + (pip_size * pip_count), pip_size_count)
        
        ensure_hedge_mode(exchange, symbol)
        
        trigger_direction = 'ascending' if side == 'buy' else 'descending'

        # Place limit buy order
        order = exchange.create_order(
            symbol=symbol,
            type=type,
            side=side,
            amount=amount,
            price=limit_price,
            params={
                'triggerPrice': limit_price,
                'triggerType': 'ByMarkPrice',
                'triggerDirection': trigger_direction,
                'reduceOnly': False,
                'closeOnTrigger': False
            }
        )

        print(f"✅ Limit buy order placed at {limit_price} and currentMarketPrice: {current_price} for {amount} {symbol}")
        limit_order_id = order.get('id')
        return limit_order_id
    except ccxt.BaseError as e:
        error_msg = str(e)
        # Handle specific phemex error for pilot contract
        if 'Pilot contract is not allowed here' in error_msg:
            thread_safe_print(f"❌ Phemex error: Pilot contract is not allowed for {symbol}. Skipping order.")
            return

        # If failed due to position mode, retry with posSide
        if 'TE_ERR_INCONSISTENT_POS_MODE' in error_msg:
            thread_safe_print("🔁 Retrying with (HEDGE LIMIT) posSide due to inconsistent position mode...")
            pos_side = 'Long' if side == 'buy' else 'Short'
            try:
                # Place limit buy order
                order = exchange.create_order(
                    symbol=symbol,
                    type=type,
                    side=side,
                    amount=amount,
                    price=limit_price,
                    params={
                        'triggerPrice': limit_price,
                        'triggerType': 'ByMarkPrice',
                        'triggerDirection': trigger_direction,
                        'reduceOnly': False,
                        'closeOnTrigger': False,
                        'posSide': pos_side
                    }
                )
                print(f"✅ Limit buy order placed at {limit_price} and currentMarketPrice: {current_price} for {amount} {symbol}")
                limit_order_id = order.get('id')
                return limit_order_id
            except ccxt.BaseError as e2:
                thread_safe_print(f"❌ Re-entry HEDGE limit order failed even with posSide: {e2}")
                return False
        else:
            thread_safe_print(f"❌ Error placing HEDGE Limit order: {e}")
            return False
        
def ensure_hedge_mode(exchange, symbol):
    try:
        exchange.set_position_mode(True, symbol=symbol)  # True = hedge mode
    except Exception as e:
        print(f"Error setting hedge mode for {symbol}: {e}")


__all__ = [name for name in globals() if not name.startswith("_")]