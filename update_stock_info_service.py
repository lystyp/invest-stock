# -*- coding: UTF-8 -*-

import utils.crawler as cl
import datetime
import utils.logging_util as logging_util
import utils.db_util as db_util
import time
import threading
import multiprocessing
import sqlalchemy
import time
import traceback
from utils.config import TABLE

log = logging_util.Logger("update_stock_info_service")

def update_daily_data(updated_cb, date):
    daily_price_thread = None
    monthly_price_thread = None
    financial_statement_thread = None

     # 收盤價
    if daily_price_thread is None or not daily_price_thread.is_alive():
        daily_price_thread = threading.Thread(cl.update_price_table(db_util.get_db_connection(), [date]))
        daily_price_thread.setDaemon(True)
    else:
        log.e("daily_price_thread is still alive ! There must be something wrong.")


    # 月報每月15日公布
    if date.day == 15:
        if monthly_price_thread is None or not monthly_price_thread.is_alive():
            monthly_price_thread = threading.Thread(cl.update_monthly_revenue_table(db_util.get_db_connection(), [date]))
            monthly_price_thread.setDaemon(True)
        else:
            log.e("monthly_price_thread is still alive ! There must be something wrong.")

    # 財報公布時間是Q1 05/15, Q2 8/14, Q3 11/14, Q4 3/31
    if (date.month == 3 and date.day == 31) or\
        (date.month == 5 and date.day == 15) or\
        (date.month == 8 and date.day == 14) or\
        (date.month == 11 and date.day == 14):
        if financial_statement_thread is None or not financial_statement_thread.is_alive():
            financial_statement_thread = threading.Thread(cl.update_finance_statement_table(db_util.get_db_connection(), [date]))
            financial_statement_thread.setDaemon(True)
        else:
            log.e("financial_statement_thread is still alive ! There must be something wrong.")

    if daily_price_thread.is_alive():
        daily_price_thread.join()
    if monthly_price_thread.is_alive():
        monthly_price_thread.join()
    if financial_statement_thread.is_alive():
        financial_statement_thread.join()
    updated_cb("Done.")

def update_newest_data():
    def price():
        conn = db_util.get_db_connection()
        today = datetime.datetime.now()
        latest_date_in_table = cl.table_latest_date(conn, TABLE.PRICE)
        date_list = cl.date_range(latest_date_in_table + datetime.timedelta(days=1), today)
        cl.update_price_table(conn,date_list)

    def monthly_revenue():
        conn = db_util.get_db_connection()
        today = datetime.datetime.now()
        latest_date_in_table = cl.table_latest_date(conn, TABLE.MONTHLY_REVENUE)
        date_list = cl.month_range(latest_date_in_table + datetime.timedelta(days=1), today)
        cl.update_monthly_revenue_table(conn,date_list)

    def financial_statement():
        conn = db_util.get_db_connection()
        today = datetime.datetime.now()
        latest_date_in_table = cl.table_latest_date(conn, TABLE.BALANCE_SHEET)
        date_list = cl.season_range(latest_date_in_table + datetime.timedelta(days=1), today)
        cl.update_finance_statement_table(conn,date_list)

    price_thread = threading.Thread(target=price)
    monthly_revenue_thread = threading.Thread(target=monthly_revenue)
    financial_statement_thread = threading.Thread(target=financial_statement)
    price_thread.setDaemon(True)
    monthly_revenue_thread.setDaemon(True)
    financial_statement_thread.setDaemon(True)
    price_thread.start()
    monthly_revenue_thread.start()
    financial_statement_thread.start()
    price_thread.join()
    monthly_revenue_thread.join()
    financial_statement_thread.join()
    log.d("Finish~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

if __name__ == '__main__':
    conn = db_util.get_db_connection()
    today = datetime.datetime.now()
    latest_date_in_table = cl.table_latest_date(conn, TABLE.MONTHLY_REVENUE)
    date_list = cl.month_range(latest_date_in_table + datetime.timedelta(days=1), today)
    cl.update_monthly_revenue_table(conn,[datetime.date(2023, 2, 15)])