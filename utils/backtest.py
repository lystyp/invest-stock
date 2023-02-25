import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import warnings
import math
from strategy import Strategy
from stock_order import Stock
import logging_util

log = logging_util.Logger("Backtest")
 
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

def backtest2(start_date, end_date, hold_days, strategy, data, cost, discount=1):
    log.d("開始回測, from " + start_date.strftime("%Y-%m-%d") + " to " + end_date.strftime("%Y-%m-%d") + ", 初始資金 :" + str(cost))
    # get price data in order backtest
    data.date = end_date
    price = data.get('收盤價', (end_date - start_date).days)
    strategy : Strategy = strategy

    own_stocks = {}
    earn_money = 0

    def trading_day(date):
        if date not in price.index:
            temp = price.loc[date:]
            if temp.empty:
                return price.index[-1]
            else:
                return temp.index[0]
        else:
            return date
    sdate = trading_day(start_date)
    edate = trading_day(sdate + datetime.timedelta(hold_days))
    while sdate < end_date:
        log.d("==================================================")
        # select stocks at date
        data.date = sdate
        sell_list  = strategy.sell_list(data, own_stocks)
        buy_list = strategy.buy_list(data)
        buy_dict = portfolio(buy_list, money=cost, data=data)
        # stocks是一個series，index是stock id，value是True
        stocks = pd.Series(data=True, index=buy_list + sell_list)

        # hold the stocks for hold_days day
        # 取得選股中一個週期中去掉開始天的所有股價
        # 為什麼要去掉開始天 ? 因為都是根據開始天的收盤價在隔天買，所以花的是隔天收盤價的錢
        s = price[stocks.index & price.columns][sdate:edate].iloc[1:]
        
        # 假如剛好開始結束日都在周末就會是空的吧我猜
        log.d(sdate.strftime("%Y-%m-%d") + " to " + edate.strftime("%Y-%m-%d"))
        if s.empty:
            log.e("無收盤資料，應為假日")
            continue
        else:
            log.d("賣出 : " + str(sell_list))
            # 先賣
            for stock_id in sell_list:
                stock : Stock = own_stocks[stock_id]
                real_price = int(s[stock_id].iloc[0] * stock.amount * 1000 * (1 - 0.001425 * discount - 0.003)) 
                cost += real_price
                own_stocks.pop(stock_id)
                log.d("賣出 : " + stock_id + ", 數量 : " + '{:.3f}'.format(stock.amount) + ", 股價 : " + str(s[stock_id].iloc[0]) 
                + ", 原股價 : " + str(stock.cost) + ", 持有天數 : " + str((sdate - stock.bought_time).days))
                earn_money += (real_price - int(stock.cost * stock.amount * 1000 * (1 + 0.001425 * discount)))
            # 再買 
            for stock_id in buy_dict.keys():
                stock = Stock(stock_id, sdate, s[stock_id].iloc[0], buy_dict[stock_id])
                real_price = int(s[stock_id].iloc[0] * stock.amount * 1000 * (1 + 0.001425 * discount))
                if stock_id in own_stocks.keys():
                    log.e(stock_id + " has already in own_stocks, there is something wrong in strategy.")
                else:
                    cost -= real_price
                    own_stocks[stock_id] = stock
                    log.d("買入 : " + stock_id + ", 數量 : " + '{:.3f}'.format(stock.amount) 
                    + ", 股價 : " + str(s[stock_id].iloc[0]) + ", 買入日期 : " + s.index[0].strftime("%Y-%m-%d"))
        for stock_id in own_stocks.keys():
            all_stocks_price += int(price[stock_id].loc[sdate] * own_stocks[stock_id].amount * 1000 * (1 - 0.001425 * discount - 0.003)) 
        log.d("目前手上持股價值 : " + str(all_stocks_price))
        log.d("餘額 : " + str(cost) + ", 損益 : " + str(earn_money))
        log.d("持股 : " + str(own_stocks.keys()))
        all_stocks_price = 0

        sdate = edate
        edate = trading_day(sdate + datetime.timedelta(hold_days)) 

    log.d("最後剩多少錢:" + str(cost))
    all_stocks_price = 0
    for stock_id in own_stocks.keys():
        all_stocks_price += int(price[stock_id].loc[sdate] * own_stocks[stock_id].amount * 1000 * (1 - 0.001425 * discount - 0.003)) 
    log.d("手上持股價值 : " + str(all_stocks_price))
    return 

# 這個是在算資金要怎麼分給這些股票
def portfolio(stock_list, money, data, lowest_fee=20, discount=0.6, odd_lot=True):
    if len(stock_list) == 0:
        return {}

    # list to series 
    stock_list = pd.Index(stock_list)
    price = data.get('收盤價', 1)
    stock_list = price.iloc[-1][stock_list].transpose()
    log.d('portfolio : estimate price according to ' +  str(price.index[-1]))

    log.d('portfolio : initial number of stock ' +  str(len(stock_list)))
    # 假如平均成本導致手續費不到20要付20元划不來，就把最貴的一支股票刪掉
    while ((money / len(stock_list)) * (0.1425/100 * discount)) < lowest_fee:
        stock_list = stock_list[stock_list != stock_list.max()]
        if len(stock_list.index) == 0:
            log.d('portfolio : after considering fee, list is empty.')
            return {}
    log.d('portfolio : after considering fee ' + str(len(stock_list)))

    if not odd_lot:
        while True:
            invest_amount = (money / len(stock_list))
            ret = np.floor(invest_amount / stock_list / 1000)
            if (ret == 0).any():
                stock_list = stock_list[stock_list != stock_list.max()]
                if len(stock_list.index) == 0:
                    log.d('portfolio : after considering odd_lot, list is empty.')
                    return {}
            else:
                break
        log.d('portfolio : after considering odd_lot ' + str(len(stock_list)))
    # 因為零股要另外下一張單，要另外花手續費，所以買不到一張的我就直接買零股，買一張以上的就以一張一張買不要零股了
    cost = round(money / len(stock_list) - 0.5)
    result = {}
    for stock_id in stock_list.index:
        # 買一張的價錢
        stock_price = stock_list.loc[stock_id] * 1000
        if stock_price > cost:
            amount = round(cost / stock_price * 1000 - 0.5) / 1000
            result[stock_id] = amount
        else:
            amount = round(cost / stock_price - 0.5)
            result[stock_id] = amount
        
    return result

if __name__ == '__main__':
    import data
    from strategy import StrategyTest

    
    d = data.Data()
    s = StrategyTest()
    backtest(datetime.date(2023, 1, 5), datetime.date(2023, 2, 10), 1, s, data=d, cost=1000000)

