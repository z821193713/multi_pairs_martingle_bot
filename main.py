"""
Created on Sun Jan 24 10:57:36 2023
"""

import time
import logging
from trader.binance_spot_trader import BinanceSpotTrader
from trader.binance_future_trader import BinanceFutureTrader
from utils import config
from apscheduler.schedulers.background import BackgroundScheduler

format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=format, filename='log.txt')
logging.getLogger("apscheduler.scheduler").setLevel(logging.WARNING)
logging.getLogger("apscheduler.executors.default").setLevel(logging.WARNING)

logger = logging.getLogger('binance')
from typing import Union
from gateway.binance_future import Interval
import numpy as np
import pandas as pd
from datetime import datetime

pd.set_option('expand_frame_repr', False)

from utils.config import signal_data


def get_data(trader: Union[BinanceFutureTrader, BinanceSpotTrader]):
    """
    根据传入的交易员对象获取数据并计算交易信号。

    Args:
        trader (Union[BinanceFutureTrader, BinanceSpotTrader]): Binance 交易员实例，可以是现货交易员或未来交易员。

    Returns:
        None: 此函数无直接返回值，但会更新全局变量 signal_data，包含交易信号的相关信息。

    """
    # 获取交易对的符号列表
    symbols = trader.symbols_dict.keys()

    signals = []

    # 如果允许列表不为空，则使用允许列表作为交易对符号列表
    if len(config.allowed_lists) > 0:
        symbols = config.allowed_lists

    for symbol in symbols:

        # 如果禁止列表不为空，并且交易对符号在禁止列表中，则跳过该交易对
        if len(config.blocked_lists) > 0:
            if symbol.upper() in config.blocked_lists:
                continue

        # 获取交易对的K线数据
        klines = trader.get_klines(symbol=symbol.upper(), interval=Interval.HOUR_1, limit=100)
        if len(klines) > 0:
            # 将K线数据转换为DataFrame格式
            df = pd.DataFrame(klines,
                              dtype=np.float64,
                              columns=['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'turnover',
                                       'a2', 'a3', 'a4', 'a5']
                              )
            # 选择需要的列
            df = df[['open_time', 'open', 'high', 'low', 'close', 'volume', 'turnover']]
            # 设置时间索引
            df.set_index('open_time', inplace=True)
            # 转换时间索引并加上8小时时差
            df.index = pd.to_datetime(df.index, unit='ms') + pd.Timedelta(hours=8)

            # 重采样数据到4小时并计算统计量
            df_4hour = df.resample(rule='4H').agg({'open': 'first',
                                                   'high': 'max',
                                                   'low': 'min',
                                                   'close': 'last',
                                                   'volume': 'sum',
                                                   'turnover': 'sum'
                                                   })

            # 打印DataFrame（可选）
            print(df)

            # 计算交易对1小时的价格变化
            pct = df['close'] / df['open'] - 1
            pct_4h = df_4hour['close'] / df_4hour['open'] - 1

            # 存储计算结果
            value = {'pct': pct[-1], 'pct_4h': pct_4h[-1], 'symbol': symbol, 'hour_turnover': df['turnover'][-1]}

            # 计算交易信号
            if value['pct'] >= config.pump_pct or value['pct_4h'] >= config.pump_pct_4h:
                # 信号1表示买入信号
                value['signal'] = 1
            elif value['pct'] <= -config.pump_pct or value['pct_4h'] <= -config.pump_pct_4h:
                value['signal'] = -1
            else:
                value['signal'] = 0

            # 将计算结果添加到信号列表中
            signals.append(value)

    # 按照价格变化率降序排序信号列表
    signals.sort(key=lambda x: x['pct'], reverse=True)
    # 更新信号数据字典
    signal_data['id'] = signal_data['id'] + 1
    signal_data['time'] = datetime.now()
    signal_data['signals'] = signals
    # 打印信号数据字典
    print(signal_data)


if __name__ == '__main__':

    config.loads('./config.json')
    print(config.blocked_lists)

    if config.platform == 'binance_spot':
        # 如果你交易的是币安现货，就设置config.platform 为 'binance_spot'，否则就交易的是币安永续合约(USDT)
        trader = BinanceSpotTrader()
    else:
        trader = BinanceFutureTrader()

    trader.get_exchange_info()
    get_data(trader)  # for testing

    scheduler = BackgroundScheduler()
    scheduler.add_job(get_data, trigger='cron', hour='*/1', args=(trader,))
    scheduler.start()

    while True:
        time.sleep(10)
        trader.start()

"""
策略逻辑: 

1. 每1个小时会挑选出前几个波动率最大的交易对(假设交易的是四个交易对).
2. 然后根据设置的参数进行下单(假设有两个仓位,那么波动率最大的两个，且他们过去一段时间是暴涨过的)
3. 然后让他们执行马丁策略.

"""
