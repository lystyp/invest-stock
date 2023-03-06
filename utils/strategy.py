from abc import ABC, abstractmethod
import talib
import pandas as pd
from . import logging_util
from . import data
import datetime
from .stock_order import Stock
import math

log = logging_util.Logger("Strategy")

class Strategy(ABC):
    
    def __init__(self):
        pass
        
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
        # set data in update function
        self.data = data.Data()

        self.close = None
        self.high = None
        self.low = None
        self.transacted_volume = None
        # 經驗法則好像從前35天前的資料開始撈出來算kd值就可以了，在更往前的資料影響不大
        self.kd_range = 35

    def buy_list(self, date):
        first_date = date - datetime.timedelta(days=self.kd_range)
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
            # 用:拿資料頭尾資料都會拿
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
                profit = (close[stock_id] - stock.price) / stock.price
                if profit > 0.02 or profit < -0.02 or (date - stock.bought_time).days > 15:
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

class StrategyTest2(Strategy):
    def __init__(self):
        # set data in update function
        self.data = data.Data()

        self.price = None
        self.pbr = None
        self.monthly_revenue = None

    def update_data(self, first_date, last_date):
        self.data.date = last_date
        n = (last_date - first_date).days + 1
        # 財報一季出一次，三個月大概六七十個營業日吧我猜，財報跟月營收除數都抓小一點避免除不夠，多拿一點也沒差
        share_capital = self.data.get('普通股股本', int(n/60) + 2)
        property = self.data.get('歸屬於母公司業主之權益合計', int(n/60) + 2) 
        self.monthly_revenue = self.data.get('當月營收', int(n/28) + 14)
        self.price = self.data.get('收盤價', n)
        self.pbr = self.price.reindex(share_capital.index, method='ffill') / (property / (share_capital / 10))

    def sell_list(self, date, stocks):
        return []
    
    def buy_list(self, date):
        # 第一個條件：股價淨值比最近一天（iloc[-1]）小於 0.5
        # 先找最近一次的pbr，再篩選出小於0.5的部分
        print(self.pbr.index)
        print(self.pbr.index <= pd.to_datetime(date))
        pbr = self.pbr[self.pbr.index <= pd.to_datetime(date)].iloc[-1]
        condition1 = pbr.index[pbr < 0.5]
        
        # 第二個條件：近三個月平均月營收 > 近一年月營收
        monthly_revenue = self.monthly_revenue[self.monthly_revenue.index <= pd.to_datetime(date)]
        condition2 = monthly_revenue.columns[monthly_revenue.iloc[-3:].mean() > monthly_revenue.iloc[-12:].mean()]
        
        l = condition1 & condition2
        return list(l[l].index)
      
class StrategyTest3(Strategy):
    def __init__(self):
        # set data in update function
        self.data = data.Data()

        self.share_capital = None
        self.price = None
        self.roc = None
        self.operating_margin = None
        self.monthly_revenue = None

    def update_data(self, first_date, last_date):
        self.data.date = last_date
        n = (last_date - first_date).days + 1

        self.share_capital = self.data.get('股本合計', int(n/60) + 2)
        # 要拿到財報當天的收盤價，最多就是剛好明天是新一季財報，間隔最常會是11-14 ~ 3/31大概有四個半月大概九十幾個交易日
        self.price = self.data.get('收盤價', (n + 120))
        
        self.free_cash_flow = \
            self.to_seasonal(self.data.get('投資活動之淨現金流入（流出）', int(n/60) + 5)) \
            + self.to_seasonal(self.data.get('營業活動之淨現金流入（流出）', int(n/60) + 5))
        self.roe = self.data.get('本期淨利（淨損）', int(n/60) + 2) / self.data.get('權益總計', int(n/60) + 2). \
            fillna(self.data.get('權益總額', int(n/60) + 2))
        self.operating_margin = self.data.get('營業利益（損失）', int(n/60) + 6)
        self.monthly_revenue = self.data.get('當月營收', int(n/30) + 5)

    def sell_list(self, date, stocks):
        return []
    
    def buy_list(self, date):
        if self.data.date < date:
            self.update_data(first_date=date, last_date=date)

        # condition 1 : 求市值
        # 股本單位是千
        share_capital = self.share_capital.loc[:date]
        market_cap = share_capital.iloc[-1] * 1000 / 10 * self.price.reindex(share_capital.index, method='ffill').iloc[-1]  # 避免當天收盤價是空的

        # condition 2 : 近四季自由現金流
        free_cash_flow = self.free_cash_flow.loc[:date].iloc[-4:].bfill().dropna(axis=1).sum()

        # condition 3 : 股東權益報酬率
        roe = self.roe.loc[:date].iloc[-1]

        # condition 4 : 與去年同季相比的營業利益成長率
        operating_margin = self.operating_margin.loc[:date]
        operating_margin_inc = (operating_margin.iloc[-1] / operating_margin.iloc[-5] - 1) * 100

        # condition 5 : 市值營收比(課程裡面是用市價 / 季營收)
        # 有些公司已經破產，導致只有舊的財報有資料，新的財報是空的，會導致在算psr的時候會除 Nan導致有無限大的值出現，需要dropna
        # 月營收單位是1000，抓最近四個月, 丟掉已經破產導致沒有月報的公司, 假設某公司某月剛好沒有月報就用最新的補
        monthly_revenue = (self.monthly_revenue.loc[:date] * 1000).iloc[-4:].bfill().dropna(axis=1) 
        season_revenue = monthly_revenue.sum()
        psr = market_cap / season_revenue

        condition1 = (market_cap < 10000000000)
        log.d("condition1 : " + str(list(condition1[condition1].index)))
        condition2 = free_cash_flow > 0
        log.d("condition2 : " + str(list(condition2[condition2].index)))
        condition3 = roe > 0
        log.d("condition3 : " + str(list(condition3[condition3].index)))
        condition4 = operating_margin_inc > 0
        log.d("condition4 : " + str(list(condition4[condition4].index)))
        condition5 = psr < 5
        log.d("condition5 : " + str(list(condition5[condition5].index)))
        # 將條件做交集（&）
        select_stock = condition1 & condition2 & condition3 & condition4 & condition5 

        return list(select_stock[select_stock].index)

    # 將每季累計的財務數據，轉換成單季，因為現金流是累加的，算個別一季的要分開算
    # 第一季財報五月出，是1~3月累加
    # 第二季財報八月出，是1~6月累加
    # 第三季財報十一月出，是1~9月累加
    # 第四季財報隔年三月出，是1~12月累加
    def to_seasonal(self, df):
        season4 = df[df.index.month == 3]
        season1 = df[df.index.month == 5]
        season2 = df[df.index.month == 8]
        season3 = df[df.index.month == 11]

        season1.index = season1.index.year
        season2.index = season2.index.year
        season3.index = season3.index.year
        season4.index = season4.index.year - 1

        newseason1 = season1
        newseason2 = season2 - season1.reindex_like(season2)
        newseason3 = season3 - season2.reindex_like(season3)
        newseason4 = season4 - season3.reindex_like(season4)

        newseason1.index = pd.to_datetime(newseason1.index.astype(str) + '-05-15')
        newseason2.index = pd.to_datetime(newseason2.index.astype(str) + '-08-14')
        newseason3.index = pd.to_datetime(newseason3.index.astype(str) + '-11-14')
        newseason4.index = pd.to_datetime((newseason4.index + 1).astype(str) + '-03-31')

        return newseason1.append(newseason2).append(newseason3).append(newseason4).sort_index()
if __name__ == '__main__':
    
    t_start = datetime.now()
    data = data.Data()
    s = StrategyTest()
    print(s.buy_list())
    print(datetime.now() - t_start)
