import shioaji as sj
import pandas as pd
import time
import json
import logging_util
import crawler


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
    conn = crawler.get_db_connection()
    # read data from sql
    df = pd.read_sql('select stock_id, date, 開盤價, 收盤價, 最高價, 最低價, 成交股數 from price where stock_id="0050"', conn,
                    index_col=['date'], parse_dates=['date'])

    # rename the columns of dataframe
    df.rename(columns={'收盤價':'close', '開盤價':'open', '最高價':'high', '最低價':'low', '成交股數':'volume'}, inplace=True)

    log.d(df.iloc[-6:-1])

