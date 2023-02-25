from abc import ABC, abstractmethod
import talib
import pandas as pd
import logging_util
import data
from datetime import datetime
from stock_order import Stock

log = logging_util.Logger("Strategy")

class Strategy(ABC):
    
    def __init__(self):
        super().__init__()
        self.execute_time  = datetime.strptime('00:00', '%H:%M').time()
        self.peroid = 1
        
    @abstractmethod
    def buy_list(self, data):
        pass

    @abstractmethod
    def sell_list(self, data, stocks):
        pass

class StrategyTest(Strategy):
    
    def buy_list(self, data):
        # KD值的第13天前KD值影響今天的成分不到1%了，所以應該只要算到13天以前就夠了，加上最高最低價區間是九天，所以抓25天資料差不多
        close = data.get('收盤價', 25)
        high = data.get('最高價', 25)
        low = data.get('最低價', 25)

        transacted_volume = pd.Series(data.get('成交金額', 1).iloc[0])
        transacted_volume = transacted_volume.sort_values(ascending=False)
        buy_list = []
        for i in range(len(transacted_volume.index)):
            # 找到成交量第三百名就不再往下找了
            if i > 300:
                break

            # 找到20支股票就不找了
            if len(buy_list) >= 20:
                break

            id = transacted_volume.index[i]
            kd = talib.STOCH(high[id].ffill().values, 
                        low[id].ffill().values, 
                        close[id].ffill().values,
                        fastk_period=9, slowk_period=3
                        , slowd_period=3, slowk_matype=1, slowd_matype=1)
        
            # kd的index是日期
            k = pd.Series(kd[0], index=close[id].index)
            d = pd.Series(kd[1], index=close[id].index)

            if (k.iloc[-1] > d.iloc[-1]) and (k.iloc[-2]  < d.iloc[-2]):
                buy_list.append(id)

        
        return buy_list

    def sell_list(self, data, stocks):
        sell_list = []
        close = data.get('收盤價', 1)
        for stock_id in stocks.keys():
            stock : Stock = stocks[stock_id]
            profit = (close[stock_id].iloc[-1] - stock.cost) / stock.cost
            if profit > 0.02 or profit < -0.02 or (close.index[-1] - stock.bought_time).days > 20:
                sell_list.append(stock_id)
        return sell_list
            

if __name__ == '__main__':
    
    t_start = datetime.now()
    data = data.Data()
    s = StrategyTest(data=data, principal=None)
    print(s.buy_list())
    print(datetime.now() - t_start)
