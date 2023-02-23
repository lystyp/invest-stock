import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import warnings
import math

warnings.simplefilter(action='ignore', category=FutureWarning)

def backtest(start_date, end_date, hold_days, strategy, data, weight='average', benchmark=None, stop_loss=None, stop_profit=None):
    
    # portfolio check
    if weight != 'average' and weight != 'price':
        print('Backtest stop, weight should be "average" or "price", find', weight, 'instead')

    # get price data in order backtest
    data.date = end_date
    price = data.get('收盤價', (end_date - start_date).days)
    # start from 1 TWD at start_date, 
    end = 1
    date = start_date
    
    # record some history
    equality = pd.Series()
    nstock = {}
    transections = pd.DataFrame()
    maxreturn = -10000
    minreturn = 10000
    
    def trading_day(date):
        if date not in price.index:
            temp = price.loc[date:]
            if temp.empty:
                return price.index[-1]
            else:
                return temp.index[0]
        else:
            return date
    
    # 就是產生從開始到結束的每個周期的日期啦，只是把list改成用yield產生
    def date_iter_periodicity(start_date, end_date, hold_days):
        date = start_date
        while date < end_date:
            yield (date), (date + datetime.timedelta(hold_days))
            date += datetime.timedelta(hold_days)

    # holddays也可以不要固定周期，自己填一個list決定多久要update一次策略選股
    def date_iter_specify_dates(start_date, end_date, hold_days):
        dlist = [start_date] + hold_days + [end_date]
        if dlist[0] == dlist[1]:
            dlist = dlist[1:]
        if dlist[-1] == dlist[-2]:
            dlist = dlist[:-1]
        for sdate, edate in zip(dlist, dlist[1:]):
            yield (sdate), (edate)
    
    if isinstance(hold_days, int):
        dates = date_iter_periodicity(start_date, end_date, hold_days)
    elif isinstance(hold_days, list):
        dates = date_iter_specify_dates(start_date, end_date, hold_days)
    else:
        print('the type of hold_dates should be list or int.')
        return None

    for sdate, edate in dates:
        
        # select stocks at date
        data.date = sdate
        # stocks是一個series，index是stock id，value是True
        stocks = strategy(data)
        
        # hold the stocks for hold_days day
        # 取得選股中一個週期中去掉開始天的所有股價
        # 為什麼要去掉開始天 ? 因為都是根據開始天的收盤價在隔天買，所以花的是隔天收盤價的錢
        s = price[stocks.index & price.columns][sdate:edate].iloc[1:]
        
        # 假如剛好開始結束日都在周末就會是空的吧我猜
        if s.empty:
            s = pd.Series(1, index=pd.date_range(sdate + datetime.timedelta(days=1), edate))
        else:
            if stop_loss != None:
                # 假如我跌了stop_loss% 就停損
                below_stop = ((s / s.bfill().iloc[0]) - 1)*100 < -np.abs(stop_loss)
                # 為什麼要shift(2) > 假如我在day n 發現要停損了，我就會用day + 1的金額賣，所以day + 2開始的金額都沒有用了，shift 2就是在把後面的價格清掉
                below_stop = (below_stop.cumsum() > 0).shift(2).fillna(False)
                s[below_stop] = np.nan
                
            if stop_profit != None:
                above_stop = ((s / s.bfill().iloc[0]) - 1)*100 > np.abs(stop_profit)
                above_stop = (above_stop.cumsum() > 0).shift(2).fillna(False)
                s[above_stop] = np.nan
                
            s.dropna(axis=1, how='all', inplace=True)
            
            # record transections
            bprice = s.bfill().iloc[0]
            sprice = s.apply(lambda s:s.dropna().iloc[-1])
            transections = transections.append(pd.DataFrame({
                'buy_price': bprice,
                'sell_price': sprice,
                'lowest_price': s.min(),
                'highest_price': s.max(),
                'buy_date': pd.Series(s.index[0], index=s.columns),
                'sell_date': s.apply(lambda s:s.dropna().index[-1]),
                'profit(%)': (sprice/bprice - 1) * 100
            }))
                        
            s.ffill(inplace=True)
                
            # calculate equality
            # normalize and average the price of each stocks
            # 看是要用百分比來算還是用價格來算，average是百分比
            if weight == 'average':
                s = s/s.bfill().iloc[0]
            s = s.mean(axis=1)
            s = s / s.bfill()[0]
        
        # print some log
        print(sdate,'-', edate, 
              '報酬率: %.2f'%( s.iloc[-1]/s.iloc[0] * 100 - 100), 
              '%', '公司數:', len(stocks))
        maxreturn = max(maxreturn, s.iloc[-1]/s.iloc[0] * 100 - 100)
        minreturn = min(minreturn, s.iloc[-1]/s.iloc[0] * 100 - 100)
        
        # plot backtest result
        ((s*end-1)*100).plot()
        # equality 就是所有決策股票的平均損益啦(可能是價格或百分比)，每個周期都會多一堆
        equality = equality.append(s*end)
        # 因為不確定前面weught是average還是price，這邊要算最後資金剩多少還是要用百分比乘本金，所以統一除於s[0]再乘本金
        end = (s/s[0]*end).iloc[-1]
        
        if math.isnan(end):
            end = 1
        
        # add nstock history
        # 表示這一個週期買了幾支股票
        nstock[sdate] = len(stocks)
        
    # 用來記錄所有週期跑下來最高盈利跟最高損益分別是多少，每個週期更新一次
    print('每次換手最大報酬 : %.2f ％' % maxreturn)
    print('每次換手最少報酬 : %.2f ％' % minreturn)
    
    # 要以哪一支股票為基準來跟自己的策略比較，這裡只有0050，要用別支的話要自己改一下
    if benchmark is None:
        benchmark = price['0050'][start_date:end_date].iloc[1:]
    
    # bechmark (thanks to Markk1227)
    ((benchmark/benchmark[0]-1)*100).plot(color=(0.8,0.8,0.8))
    plt.ylabel('Return On Investment (%)')
    plt.grid(linestyle='-.')
    plt.show()
    ((benchmark/benchmark.cummax()-1)*100).plot(legend=True, color=(0.8,0.8,0.8))
    ((equality/equality.cummax()-1)*100).plot(legend=True)
    plt.ylabel('Dropdown (%)')
    plt.grid(linestyle='-.')
    plt.show()
    pd.Series(nstock).plot.bar()
    plt.ylabel('Number of stocks held')
    return equality, transections

# 這個是在算資金要怎麼分給這些股票
def portfolio(stock_list, money, data, lowest_fee=20, discount=0.6, add_cost=10):
    price = data.get('收盤價', 1)
    stock_list = price.iloc[-1][stock_list].transpose()
    print('estimate price according to', price.index[-1])

    print('initial number of stock', len(stock_list))
    while (money / len(stock_list)) < (lowest_fee - add_cost) * 1000 / 1.425 / discount:
        stock_list = stock_list[stock_list != stock_list.max()]
    print('after considering fee', len(stock_list))
        
    while True:
        invest_amount = (money / len(stock_list))
        ret = np.floor(invest_amount / stock_list / 1000)
        
        if (ret == 0).any():
            stock_list = stock_list[stock_list != stock_list.max()]
        else:
            break
    
    print('after considering 1000 share', len(stock_list))
        
    return ret, (ret * stock_list * 1000).sum()