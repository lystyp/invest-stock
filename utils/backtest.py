import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import warnings
import math
import traceback
from .strategy import Strategy
from .stock_order import Stock
from . import logging_util



log = logging_util.Logger("Backtest")
 
HANDLING_FEE = 0.001425
TAX = 0.003 

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
    try:
        log.d("開始回測, from " + start_date.strftime("%Y-%m-%d") + " to " + end_date.strftime("%Y-%m-%d") + ", 初始資金 :" + str(cost))
        # get price data in order backtest
        data.date = end_date
        price = data.get('收盤價', (end_date - start_date).days)
        strategy : Strategy = strategy

        own_stocks = {}
        earn_money = 0
        win_count = 0
        lose_count = 0
        no_deal_count = 0

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
            sell_list  = strategy.sell_list(sdate, own_stocks)
            buy_list = strategy.buy_list(sdate)
            # stocks是一個series，index是stock id，value是True
            stocks = pd.Series(data=True, index=buy_list + sell_list)

            # hold the stocks for hold_days day
            # 取得選股中一個週期中去掉開始天的所有股價
            # 為什麼要去掉開始天 ? 因為都是根據開始天的收盤價在隔天買，所以花的是隔天收盤價的錢
            # 這是用來表示這一輪需要買賣的股票的收盤價，不包含其他天買賣的股票
            s = price[stocks.index & price.columns][sdate:edate].iloc[1:]
            
            # 假如剛好開始結束日都在周末就會是空的吧我猜
            log.d(sdate.strftime("%Y-%m-%d") + " to " + edate.strftime("%Y-%m-%d"))
            if stocks.empty:
                log.w("無符合買賣條件之股票")
            elif s.empty:
                log.w("無收盤資料，應為假日")
            else:
                # 先賣
                log.d("賣出 : " + str(sell_list))
                for stock_id in sell_list:
                    stock : Stock = own_stocks[stock_id]
                    if math.isnan(s[stock_id].iloc[0]):
                        cost += get_totally_selling_price(stock.price, stock.amount, discount)
                        own_stocks.pop(stock_id)
                        log.e("賣出 : " + stock_id + ", 數量 : " + str(stock.amount) + 
                              ", 股價為空，可能無人賣出或是股票減資出關，原價賣出") 
                        no_deal_count += 1
                    else:
                        real_price = get_totally_selling_price(s[stock_id].iloc[0], stock.amount, discount)
                        earn_money += (real_price - stock.total_cost)
                        
                        cost += real_price
                        own_stocks.pop(stock_id)
                        log.d("賣出 : " + stock_id + ", 數量 : " + str(stock.amount) + ", 股價 : " + str(s[stock_id].iloc[0]) 
                        + ", 原股價 : " + str(stock.price) + ", 持有天數 : " + str((sdate - stock.bought_time).days))
                        if s[stock_id].iloc[0] > stock.price:
                            win_count += 1
                        else:
                            lose_count += 1
                # 再買 
                buy_dict = portfolio(buy_list, money=cost, prices=price.loc[sdate].dropna())
                log.d("買入 : " + str(buy_dict.keys()))
                for stock_id in buy_dict.keys():
                    if math.isnan(s[stock_id].iloc[0]):
                        log.w("買入 : " + stock_id + ", 股價為空，可能無人賣出或是股票減資出關，不買入") 
                        no_deal_count += 1
                    else:
                        buy_price = get_totally_buying_price(s[stock_id].iloc[0], buy_dict[stock_id], discount)
                        stock = Stock(stock_id, sdate, s[stock_id].iloc[0], buy_price, buy_dict[stock_id])
                        if buy_price > cost:
                            log.w("買入 : " + stock_id + ", 數量 : " + str(stock.amount)
                            + ", 股價 : " + str(s[stock_id].iloc[0]) + ", 技術分析時股價 : " + str(price[stock_id].loc[sdate])
                              + ", 餘額不足，剩 : " + str(cost))
                            no_deal_count += 1
                        else:
                            cost -= buy_price
                            if stock_id in own_stocks.keys():
                                own_stocks[stock_id].add(stock)
                            else:
                                own_stocks[stock_id] = stock
                            log.d("買入 : " + stock_id + ", 數量 : " + str(stock.amount) 
                                + ", 股價 : " + str(s[stock_id].iloc[0]) + ", 技術分析時股價 : " + str(price[stock_id].loc[sdate])
                                + ", 買入日期 : " + s.index[0].strftime("%Y-%m-%d"))
            
                all_stocks_price = 0
                all_stocks_price_origin = 0
                for stock_id in own_stocks.keys():
                    p = own_stocks[stock_id].price if math.isnan(price[stock_id].loc[edate]) else price[stock_id].loc[edate]
                    all_stocks_price += get_totally_selling_price(p, own_stocks[stock_id].amount, discount)
                    all_stocks_price_origin += own_stocks[stock_id].total_cost
                  
                log.d("週期結束時目前手上持股價值 : " + str(all_stocks_price) + ", 原價 : " + str(all_stocks_price_origin))
                log.d("餘額 : " + str(cost) + ", 損益 : " + str(earn_money) + ", 總資產 : " + str(cost + all_stocks_price))
                log.d("持股 : " + str(own_stocks.keys()))
        
            sdate = edate
            edate = trading_day(sdate + datetime.timedelta(hold_days)) 
            # sdate == edate表示已經沒有更新的資料了，可以停止回測了
            if sdate == edate:
                break
        log.d("========結算=======")
        all_stocks_price = 0
        all_stocks_price_origin = 0
        for stock_id in own_stocks.keys():
            p = own_stocks[stock_id].price if math.isnan(price[stock_id].loc[edate]) else price[stock_id].loc[edate]
            all_stocks_price += get_totally_selling_price(p, own_stocks[stock_id].amount, discount)
            all_stocks_price_origin += own_stocks[stock_id].total_cost
            log.d("持股 : " + str(stock_id) + ", 數量 : " + str(own_stocks[stock_id].amount) + ", 股價 : " + str(price[stock_id].iloc[0]) 
                        + ", 原股價 : " + str(own_stocks[stock_id].price) + ", 買入日期 : " + str(own_stocks[stock_id].bought_time))
        log.d("手上持股價值 : " + str(all_stocks_price) + ", 原價 : " + str(all_stocks_price_origin))
        log.d("餘額 : " + str(cost) + ", 損益 : " + str(earn_money) + ", 總資產 : " + str(cost + all_stocks_price))
        log.d("賺:" + str(win_count) + ", 賠:" + str(lose_count) + ", 成交失敗:" + str(no_deal_count))
    except Exception:
        log.e("Error : " + str(traceback.format_exc()))
    return 

def get_totally_buying_price(price, amount, discount):
    return int(price * amount * 1000 * (1 - HANDLING_FEE * discount))
    
def get_totally_selling_price(price, amount, discount):
    # int是無條件捨去，如果有零股要跟整張分開賣
    if (amount > 1) and (amount - int(amount) > 0):
        return int(price * amount * 1000 * (1 - 2*(HANDLING_FEE * discount + TAX))) 
    else:
        return int(price * amount * 1000 * (1 - (HANDLING_FEE * discount + TAX))) 

# 這個是在算資金要怎麼分給這些股票
def portfolio(stock_list, money, prices, lowest_fee=20, discount=1, odd_lot=True):
    if len(stock_list) == 0:
        return {}

    # list to series 
    stock_list = pd.Index(stock_list)
    stock_list = prices[stock_list].transpose()
    log.d('portfolio : initial number of stock ' +  str(len(stock_list)) + ", money : " + str(money))
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
    cost = int(money / len(stock_list))
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

