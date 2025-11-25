import time
import traceback
import warnings
from collections import deque
from datetime import datetime, timedelta

import pandas as pd

from config import error_webhook_url
from core.account_manager import init_system, load_multi_accounts
from core.binance.base_client import BinanceClient
from core.trade import split_order_twap
from core.utils.notification import send_wechat_work_msg

warnings.filterwarnings('ignore')
pd.set_option('display.max_rows', 1000)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)

monitor_time = 60  # monitor frequency (second)

# --- margin rate circuit breaker ---
enable_margin_rate_circuit_breaker = True
stop_equity_pnl = 0.5       # trigger
stop_rate = 0.1             # decrease position per monitor timer

# --- drawdown circuit breaker ---
enable_drawdown_circuit_breaker = True
drawdown_period_minutes = 60    # drawdown period
drawdown_threshold = 0.10       # threshold

cli = BinanceClient.get_dummy_client()
equity_history_dict = dict()

def run():
    while True:
        for account_profile_file in load_multi_accounts():
            acct_cfg, me_conf = init_system(account_profile_file.stem)
            acct_cfg.update_account_info()

            for account_info in [acct_cfg]:
                account_name = account_info.name
                swap_position = account_info.swap_position
                swap_equity = account_info.swap_equity
                max_one_order_amount = account_info.max_one_order_amount
                twap_interval = account_info.twap_interval

                if swap_position.empty:
                    print('no position, skip')
                    continue

                total_equity = swap_equity
                triggered_drawdown = False
                if enable_drawdown_circuit_breaker:
                    if account_name not in equity_history_dict:
                        equity_history_dict[account_name] = deque()
                    history = equity_history_dict[account_name]
                    now = datetime.now()
                    history.append((now, total_equity))
                    min_time = now - timedelta(minutes=drawdown_period_minutes)
                    while history and history[0][0] < min_time:
                        history.popleft()
                    if history:
                        window_max_equity = max(x[1] for x in history)
                        if window_max_equity > 0:
                            drawdown = (window_max_equity - total_equity) / window_max_equity
                            print(f"[drawdown circuit breaker] account {account_name} latest {drawdown_period_minutes} min max equity ={window_max_equity:.2f}，current equity={total_equity:.2f}，drawdown% ={drawdown:.3%}")
                            if drawdown >= drawdown_threshold:
                                triggered_drawdown = True
                                send_wechat_work_msg(
                                    f'[drawdown circuit breaker] account {account_name} latest {drawdown_period_minutes} min max drawdown {drawdown:.2%}，now trigger decrease position action',
                                    account_info.wechat_webhook_url
                                )

                triggered_margin = False
                pos_equity = (swap_position['当前标记价格'] * swap_position['当前持仓量']).abs().sum()
                if pos_equity > 0:
                    margin_rate = total_equity / pos_equity
                else:
                    margin_rate = float('inf')
                print(f'current equity: {total_equity}，swap equity: {pos_equity}，margin rate: {margin_rate}')
                if enable_margin_rate_circuit_breaker and margin_rate <= stop_equity_pnl:
                    triggered_margin = True
                    print(f"[margin circuit breaker] margin rate {margin_rate} <= monitor rate {stop_equity_pnl}")

                if triggered_drawdown or triggered_margin:
                    swap_position['order qty'] = swap_position['当前持仓量'] * stop_rate * -1
                    swap_position['order amount'] = swap_position['order qty'] * swap_position['当前标记价格']
                    swap_position['mode'] = 'decrease position'
                    swap_position = swap_position[abs(swap_position['order qty']) > 0]
                    swap_position.reset_index(inplace=True)
                    swap_position['symbol_type'] = 'swap'
                    print('swap order inf：\n', swap_position)

                    if not swap_position.empty:
                        cli.get_market_info(symbol_type='swap', require_update=True)
                        swap_orders_df_list = split_order_twap(swap_position, max_one_order_amount)
                        for i in range(len(swap_orders_df_list)):
                            account_info.bn.place_swap_orders_bulk(swap_orders_df_list[i])
                            print(f'sleep {twap_interval}s then continue to place order')
                            time.sleep(twap_interval)

                    account_info.update_account_info(is_only_spot_account=True)
                    account_info.bn.collect_asset()
                    send_wechat_work_msg(f'decrease position completed', account_info.wechat_webhook_url)
                    time.sleep(2)
                else:
                    pass

        print('-' * 20, f'current round of monitor finished，will start next round in {monitor_time}s', '-' * 20)
        print('\n')
        time.sleep(monitor_time)

if __name__ == '__main__':

    while True:
        try:
            run()
        except KeyboardInterrupt:
            print('exit')
            exit()
        except Exception as err:
            msg = 'program error，run in 10s，error msg: ' + str(err)
            print(msg)
            print(traceback.format_exc())
            send_wechat_work_msg(msg, error_webhook_url)
            time.sleep(12)
