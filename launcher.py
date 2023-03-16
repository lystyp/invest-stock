# -*- coding: UTF-8 -*-
import update_stock_info_service
import datetime
from utils.logging_util import Logger
import threading
import time
log = Logger("Launcher")

daily_update_time = None

def updated_cb(msg):
    log.d("Data is updated : ", msg)
    daily_update_time = True

if __name__ == '__main__':
    thr = None
    while(True):
        now = datetime.datetime.now()
        # 每天晚上十一點更新
        if now.hour == 23:
            log.d("Today is ", now)
            if daily_update_finished is not None and ( 
                daily_update_time.year == now.year and 
                daily_update_time.month == now.month and 
                daily_update_time.day == now.day) :
                log.d("Today's info has been updated.")
            elif thr is not None and thr.is_alive():
                log.e("update_stock_info_service is still running, something wrong.")
            else:
                daily_update_time = now
                thr = threading.Thread(target=update_stock_info_service.update_daily_data, args=(updated_cb, now))
                thr.setDaemon(True)
                thr.start()
        log.d("Just show a useless log ...")
        time.sleep(600)
        