from BsObject import DataHandler
from BsEvent import MarketEvent
import os
import pandas as pd
import time
import threading
import datetime
import pymysql


class MysqlDataHandler(DataHandler):
    """
    MysqlDataHandler
    """

    def __init__(self, event_queue, symbol, market, start_date, end_date=datetime.date.today(), replay_speed=0.01, frequency='1d'):
        """
        DataHandler初始化
        """
        self.symbol = symbol
        self._parse_mysql_files(symbol, market, start_date, end_date, frequency)
        self.continue_backtest = True
        self.__cursor = 0
        self.event_queue = event_queue  # 事件队列
        self.replay_speed = replay_speed  # 行情回放速度
        self.run_thread = None  # __run()线程

    def __run(self):
        while self.continue_backtest:
            if self.__cursor == len(self.__data) - 1:
                self.continue_backtest = False
            else:
                self.__cursor += 1
            self.event_queue.put(MarketEvent())  # 生成 MareketEvent
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

    def _parse_mysql_files(self, symbol, market, start_date, end_date, frequency):
        """
        parse csv files, save to self.__data

        :param file_dir:  ./data/IF.csv
        :return: self.date
        """
        conn = pymysql.connect(host='geekthings-oregon.cewoxtkdnkzf.us-west-2.rds.amazonaws.com', port=3306,
                               user='geekthings',
                               passwd='geekthings', db='gtdcmdb')
        df = pd.read_sql_query("SELECT * FROM `Market_Kline_His` where symbol = '%s' and period = '%s' "
                               "and gmt_create >= '%s' and from_name = '%s'" %
                               (symbol, frequency, start_date, market), con=conn)
        df.reset_index(inplace=True, drop=True)
        self.__data = df


if __name__ == '__main__':
    from queue import Queue
    event_q = Queue()
    M = MysqlDataHandler(event_q, symbol='BTCUSDT', market='Binance', start_date='2018-07-11')
    M.run()

