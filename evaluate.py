import itertools

import numpy as np
import pandas as pd


def strategy_evaluate(equity, net_col='多空资金曲线', pct_col='本周期多空涨跌幅'):

    results = pd.DataFrame()

    def num_to_pct(value):
        return '%.2f%%' % (value * 100)

    results.loc[0, 'Cumulative Return'] = round(equity[net_col].iloc[-1], 2)

    annual_return = (equity[net_col].iloc[-1]) ** (
            '1 days 00:00:00' / (equity['candle_begin_time'].iloc[-1] - equity['candle_begin_time'].iloc[0]) * 365) - 1
    results.loc[0, 'Annual Return'] = num_to_pct(annual_return)


    equity[f'{net_col.split("Equity Curve")[0]}max2here'] = equity[net_col].expanding().max()

    equity[f'{net_col.split("Equity Curve")[0]}dd2here'] = equity[net_col] / equity[f'{net_col.split("Equity Curve")[0]}max2here'] - 1

    end_date, max_draw_down = tuple(equity.sort_values(by=[f'{net_col.split("Equity Curve")[0]}dd2here']).iloc[0][['candle_begin_time', f'{net_col.split("Equity Curve")[0]}dd2here']])

    start_date = equity[equity['candle_begin_time'] <= end_date].sort_values(by=net_col, ascending=False).iloc[0]['candle_begin_time']
    results.loc[0, 'Max Drawdown'] = num_to_pct(max_draw_down)
    results.loc[0, 'Max Drawdown Start'] = str(start_date)
    results.loc[0, 'Max Drawdown End'] = str(end_date)

    results.loc[0, 'Annual Return/ Max Drawdown'] = round(annual_return / abs(max_draw_down), 2)

    results.loc[0, 'Positive Return Num'] = len(equity.loc[equity[pct_col] > 0])
    results.loc[0, 'Negative Return Num'] = len(equity.loc[equity[pct_col] <= 0])
    results.loc[0, 'Win Rate'] = num_to_pct(results.loc[0, 'Positive Return Num'] / len(equity))
    results.loc[0, 'Average Return per Period'] = num_to_pct(equity[pct_col].mean())
    results.loc[0, 'Profit-to-Loss Ratio'] = round(equity.loc[equity[pct_col] > 0][pct_col].mean() / equity.loc[equity[pct_col] <= 0][pct_col].mean() * (-1), 2)
    if 1 in equity['是否爆仓'].to_list():
        results.loc[0, 'Profit-to-Loss Ratio'] = 0
    results.loc[0, 'Maximum Single-Period Profit'] = num_to_pct(equity[pct_col].max())
    results.loc[0, 'Maximum Single-Period Loss'] = num_to_pct(equity[pct_col].min())


    results.loc[0, 'Max Consecutive Winning Periods'] = max(
        [len(list(v)) for k, v in itertools.groupby(np.where(equity[pct_col] > 0, 1, np.nan))])
    results.loc[0, 'Max Consecutive Losing Periods'] = max(
        [len(list(v)) for k, v in itertools.groupby(np.where(equity[pct_col] <= 0, 1, np.nan))])

    results.loc[0, 'Return Volatility (Standard Deviation)'] = num_to_pct(equity[pct_col].std())

    # === Sharpe Ratio (1h -> 1 year) ===
    risk_free_rate = 0
    hours_per_year = 365 * 24

    hourly_returns = equity[pct_col].dropna()
    excess_return = hourly_returns - (risk_free_rate / hours_per_year)

    mean_r = excess_return.mean()
    std_r = excess_return.std()

    if std_r != 0:
        hourly_sharpe = mean_r / std_r
        annual_sharpe = hourly_sharpe * np.sqrt(hours_per_year)
    else:
        annual_sharpe = np.nan

    results.loc[0, 'Sharpe Ratio'] = round(annual_sharpe, 2)

    temp = equity.copy()
    temp.set_index('candle_begin_time', inplace=True)
    year_return = temp[[pct_col]].resample(rule='A').apply(lambda x: (1 + x).prod() - 1)
    month_return = temp[[pct_col]].resample(rule='M').apply(lambda x: (1 + x).prod() - 1)
    quarter_return = temp[[pct_col]].resample(rule='Q').apply(lambda x: (1 + x).prod() - 1)

    def num2pct(x):
        if str(x) != 'nan':
            return str(round(x * 100, 2)) + '%'
        else:
            return x

    year_return['Return'] = year_return[pct_col].apply(num2pct)
    month_return['Return'] = month_return[pct_col].apply(num2pct)
    quarter_return['Return'] = quarter_return[pct_col].apply(num2pct)

    return results.T, year_return, month_return, quarter_return
