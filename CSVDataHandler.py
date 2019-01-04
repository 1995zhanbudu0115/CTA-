from BsObject import DataHandler
from BsEvent import MarketEvent
import os
import pandas as pd
import time
import threading


class CSVDataHandler(DataHandler):
    """
    CSVDataHandler
    """

    def __init__(self, event_queue, symbol, frequency, replay_speed=0):
        """
        DataHandler初始化
        """
        self._import_history_data(symbol, frequency)
        self.continue_backtest = True
        self.__cursor = 0
        self.event_queue = event_queue     # 事件队列
        self.replay_speed = replay_speed   # 行情回放速度
        self.run_thread = None             # __run()线程

    def __run(self):
        while self.continue_backtest:
            if self.__cursor == len(self.__data) - 1:
                self.continue_backtest = False
            else:
                self.__cursor += 1
            self.event_queue.put(MarketEvent())   # 生成 MareketEvent
            print('>>>>>>>>>>>>>>>>>>>>> putting MarketEvent <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<')
            time.sleep(self.replay_speed)

    def run(self):
        self.run_thread = threading.Thread(target=self.__run)
        self.run_thread.start()

    def get_prev_bars(self, n=1, columns=None):
        """
        interface for accessing to values of **previous** n bars. Select field(s) by columns, all the columns is selected by default

        :param n:
        :param columns:
        :return: pd.Series if columns is scalar, pd.DataFrame if columns is vector
        """
        if columns is None:
            columns = self.__data.columns.values
        return self.__data.loc[(self.__cursor - n):self.__cursor, columns]

    def get_current_bar(self, columns=None):
        """
        interface for accessing to **current** bar. Select field(s) by columns, all the columns is selected by default

        :param columns:
        :return: pd.Series if columns is vector
        """
        if columns is None:
            columns = self.__data.columns.values
        return self.__data.loc[self.__cursor, columns]

    def _import_history_data(self, symbol, frequency):
        """
        获取本地历史行情数据，时长为bitfinex， 主要区间为2017年， 频率为1T, 5T, 15T, 30T, 1H
        :param symbol: str 标的名称,大写 eg: BTCUSD
        :param period: str 频率 eg: 1T means 1min
        :return:
        """
        t_da = pd.DataFrame()
        days = os.listdir('D:\my_files_du\my_project\py\geekthings\coin_data\Data')
        for d in days:
            num = d.split('-')[0] + d.split('-')[1] + d.split('-')[2]
            da = pd.read_csv(
                'D:\my_files_du\my_project\py\geekthings\coin_data\Data\\{0}\\{1}\\BITFINEX_{2}_{3}_{4}.csv'
                .format(d, symbol, symbol, num, frequency), header=1, names=['gmt_create', 'open', 'high', 'low',
                                                                             'close', 'vol'])
            t_da = t_da.append(da)
        t_da.reset_index(inplace=True, drop=True)
        self.__data = t_da


if __name__ == '__main__':
    from queue import Queue
    event_q = Queue()
    M = CSVDataHandler(event_q, symbol='BTCUSD', frequency='1h')
    M.run()