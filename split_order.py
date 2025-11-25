import random
import math
import numpy as np
import pandas as pd

def _round_by_type(value: float, precision: int, symbol_type: str) -> float:

    _m = 10 ** precision
    if symbol_type == 'swap':
        return value / abs(value) * math.ceil(abs(value) * _m) / _m
    else:
        if value > 0:
            return math.ceil(value * _m) / _m
        else:
            return -math.floor(abs(value) * _m) / _m


def _random_split(x, order_amount_limit):
    if abs(x) < order_amount_limit:
        return [x]

    result = []
    remaining = abs(x)

    while remaining > 0:
        random_amount = random.uniform(0.5 * order_amount_limit, 1.2 * order_amount_limit)
        amount = min(random_amount, remaining)
        result.append(amount if x > 0 else -amount)
        remaining -= amount

    return result

def split_order_twap(orders_df: pd.DataFrame, order_amount_limit):

    original_order_amount = orders_df['order qty'].copy()

    orders_df['拆单金额'] = orders_df['order amount'].apply(lambda x: _random_split(x, order_amount_limit))
    orders_df['拆单金额'] = orders_df['拆单金额'].apply(np.array)
    orders_df['order qty'] = orders_df['order qty'] / orders_df['order amount'] * orders_df['拆单金额']

    from core.binance.base_client import BinanceClient

    for idx, row in orders_df.iterrows():
        symbol = row['symbol']
        original_symbol = symbol
        symbol = symbol[:-4] + '/' + symbol[-4:]
        symbol_type = row['symbol_type']
        if symbol_type not in BinanceClient.market_info:
            BinanceClient().fetch_market_info(symbol_type)
        market_info = BinanceClient.market_info[symbol_type]
        min_qty = market_info['min_qty']
        if original_symbol not in min_qty:
            print(f"symbol {symbol} not in min_qty")
            continue

        remaining_amount = original_order_amount[idx]
        split_amounts = orders_df.loc[idx, 'order qty']
        precision = min_qty[original_symbol]

        total_after_round = sum(_round_by_type(amt, precision, symbol_type) for amt in split_amounts[:-1])

        if len(split_amounts) > 0:
            last_amount = remaining_amount - total_after_round
            if (last_amount * split_amounts[0] < 0) or abs(last_amount) < 10**(-precision):
                split_amounts = split_amounts[:-1]
                if len(split_amounts) > 0:
                    split_amounts[-1] += last_amount
            else:
                split_amounts[-1] = last_amount

    orders_df.reset_index(inplace=True)
    del orders_df['拆单金额']

    orders_df = orders_df.explode('order qty')

    twap_orders_df_list = []

    group = orders_df.groupby(by='index')

    max_group_len = group['index'].size().max()
    if max_group_len > 0:

        for i in range(max_group_len):
            twap_orders_df_list.append(
                group.nth(i).sort_values('order amount', ascending=False).reset_index(drop=True))

    return twap_orders_df_list
