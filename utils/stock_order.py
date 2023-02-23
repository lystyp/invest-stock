import shioaji as sj
import pandas as pd
import time
import json
import logging_util
import crawler
import sqlalchemy
from talib import abstract
from db_util import get_db_connection
import matplotlib.pyplot as plt

log = logging_util.Logger("stock order")

def get_recommand_order_list(category):
    # 回傳根據策略建議的股票，回傳一個list或是dict
    pass

def get_daily_order(recommand_order):
    # 拿到推薦股票後，看那些股是已經有買的，哪些是還沒買的，錢還剩多少可以買，要賣哪些，總之這裡就是看錢要怎麼安排
    pass

if __name__ == '__main__':
    # login_info = {}
    # with open('../authentication/shioaji_login_information.json', 'r') as file:
    #     login_info = json.load(file)

    # api = sj.Shioaji(simulation=False) 
    # accounts =  api.login(login_info["api_key"], login_info["secret_key"])
    # api.activate_ca(
    #     ca_path="../authentication/Sinopac.pfx",
    #     ca_passwd=login_info["ca_passwd"],
    #     person_id=login_info["person_id"],
    # )

    # save = api.account_balance()

    import data
    import talib
    import matplotlib.pyplot as plt
    import datetime

    d = data.Data()
    close = d.get('收盤價', 500)
    high = d.get('最高價', 500)
    low = d.get('最低價', 500)


    def k_buy_d_sale(id):
        kd = talib.STOCH(high[id].ffill().values, 
                        low[id].ffill().values, 
                        close[id].ffill().values,
                        fastk_period=9, slowk_period=3
                        , slowd_period=3, slowk_matype=1, slowd_matype=1)
        
        k = pd.Series(kd[0], index=close[id].index)
        d = pd.Series(kd[1], index=close[id].index)

        buy = (k > d) & (k.shift(1) < d.shift(1)) 
        buy = buy[buy]
        sale = (k < d) & (k.shift(1) > d.shift(1))
        sale = sale[sale]
        if buy.index[0] > sale.index[0]:
            sale = sale.drop([sale.index[0]])


        log.d(id)
        df = pd.DataFrame({"d":[], "p":[], "high_p":[], "k_buy":[], "k_sale":[]})
  
        for i in range(len(sale.index)):
            distance = sale.index[i] - buy.index[i]
            p_buy = close[id].shift(-1).loc[buy.index[i]]
            p_sale = close[id].shift(-1).loc[sale.index[i]]
            inc = (p_sale - p_buy) / p_buy * 100
            high_p = (close[id].shift(-1).loc[buy.index[i]:sale.index[i]].max() - p_buy) * 100 / p_buy 
            df.loc[len(df)] = [distance, inc, high_p, k.loc[buy.index[i]], k.loc[sale.index[i]]]
        log.d(df.sort_values(by="p"))
        log.d("======")

    def k_buy_1_sale(id):
        kd = talib.STOCH(high[id].ffill().values, 
                        low[id].ffill().values, 
                        close[id].ffill().values,
                        fastk_period=9, slowk_period=3
                        , slowd_period=3, slowk_matype=1, slowd_matype=1)
        
        k = pd.Series(kd[0], index=close[id].index)
        d = pd.Series(kd[1], index=close[id].index)
        t_buy = (k > d) & (k.shift(1) < d.shift(1))
        df = pd.DataFrame({"id":[], "date_buy":[], "days":[], "k_buy":[], "k_sale":[]})
    
        for i in range(len(t_buy.index)):
            if t_buy.iloc[i]:
                d_buy = t_buy.index[i+1]
                d_sale = None

                price_buy = close[id].iloc[i+1]
                price_sale = price_buy * 1.01
                for j in range(i+2, len(t_buy.index)):
                    if close[id].iloc[j] > price_sale:
                        d_sale = t_buy.index[j]
                        break
                df.loc[len(df)] = [id, d_buy, None if d_sale is None else (d_sale - d_buy).days, k.loc[d_buy], None if d_sale is None else k.loc[d_sale]]
        
        return df



    df1 = k_buy_1_sale("2330")
    df2 = k_buy_1_sale("3443")
    df3 = k_buy_1_sale("2603")
    df4 = k_buy_1_sale("1513")
    df5 = k_buy_1_sale("6531")
    df_all = pd.concat([df1, df2, df3, df4, df5])
    df_all.sort_values("date_buy", inplace=True)
    log.d(df_all.to_string())
    df_all["days"].hist()
    plt.show()
