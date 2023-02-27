from abc import ABC, abstractmethod
import talib
import pandas as pd
from . import logging_util
from . import data
from datetime import datetime, timedelta
from .stock_order import Stock
import math

log = logging_util.Logger("Strategy")

class Strategy(ABC):
    
    def __init__(self):
        super().__init__()
        
    @abstractmethod
    def buy_list(self, date):
        pass

    @abstractmethod
    def sell_list(self, date, stocks):
        pass

    @abstractmethod
    def update_data(self, start_date, end_date):
        pass


class StrategyTest(Strategy):
    def __init__(self):
        super().__init__()
        # set data in update function
        self.data = data.Data()
        self.close = None
        self.high = None
        self.low = None
        self.transacted_volume = None
        # 經驗法則好像從前35天前的資料開始撈出來算kd值就可以了，在更往前的資料影響不大
        self.kd_range = 35

    def buy_list(self, date):
        first_date = date - timedelta(days=self.kd_range)
        last_date = date
        # 假如我要分析的日期比data的date新，那應該是
        if self.data.date < date:
            # 雖然KD值需要用到前n天的資料，但我參數一樣頭跟尾都丟今天就好了，n天在update裡面會去做
            self.update_data(first_date=last_date, last_date=last_date)

        transacted_volume = pd.Series(self.transacted_volume.loc[last_date])
        transacted_volume.sort_values(ascending=False, inplace=True)
        buy_list = []
        for i in range(len(transacted_volume.index)):
            # 找到成交量第三百名就不再往下找了
            if i > 300:
                break

            # 找到20支股票就不找了
            if len(buy_list) >= 20:
                break

            id = transacted_volume.index[i]
            # 頭尾資料都會拿
            high = self.high[id].loc[first_date:last_date].ffill()
            low = self.low[id].loc[first_date:last_date].ffill()
            close = self.close[id].loc[first_date:last_date].ffill()
            kd = talib.STOCH(high.values, low.values, close.values, 
                             fastk_period=9, 
                             slowk_period=3, slowd_period=3, 
                             slowk_matype=1, slowd_matype=1)
            # kd的index是日期
            k = pd.Series(kd[0], index=high.index)
            d = pd.Series(kd[1], index=high.index)
            if (k.iloc[-1] > d.iloc[-1]) and (k.iloc[-2]  < d.iloc[-2]):
                buy_list.append(id)
        return buy_list

    def sell_list(self, date, stocks):
        if self.data.date < date:
            self.update_data(first_date=date, last_date=date)

        sell_list = []
        close = self.close.loc[date]
        for stock_id in stocks.keys():
            if math.isnan(close[stock_id]):
                log.e("不明原因，股票 : " + stock_id + " 收盤價為空")
            else:
                stock : Stock = stocks[stock_id]
                profit = (close[stock_id] - stock.cost) / stock.cost
                if profit > 0.03 or profit < -0.01 or (date - stock.bought_time).days > 14:
                    sell_list.append(stock_id)
        return sell_list
    
    # 這個function是用來去DB撈新的資料出來的，把DB操作單獨拉出來做一個function，避免sell跟buy在回測時每測一次就要撈一次DB很浪費時間
    # 可以直接要用來回測的資料一次全部撈出來
    # first_date:我預期要分析的日期區間的第一天, last_date:我預期要分析的日期區間的最後一天
    # 兩個用處 : 
    # 1. 每天更新資料，像這個策略我就是每天更新從25天前到今天的資料給sell跟buy方法用
    # 2. backtest.py做回測時一次更新從回測的起始天到結束天的所有資料
    def update_data(self, first_date, last_date):
        self.data.date = last_date
        # 算KD值需要不包含今天的前12天資料(算法之後再慢慢搞懂)
        n = (last_date - first_date).days + self.kd_range 
        
        self.close = self.data.get('收盤價', n)
        self.high = self.data.get('最高價', n)
        self.low = self.data.get('最低價', n)
        self.transacted_volume = self.data.get('成交金額', n)

if __name__ == '__main__':
    
    t_start = datetime.now()
    data = data.Data()
    s = StrategyTest()
    print(s.buy_list())
    print(datetime.now() - t_start)
