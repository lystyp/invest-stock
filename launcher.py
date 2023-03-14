import update_stock_info_service
import datetime
from utils.logging_util import Logger
import threading
import time
log = Logger("Launcher")


def updated_cb(msg):
    log.d("Data is updated : ", msg)

if __name__ == '__main__':
    thr = None
    while(True):
        now = datetime.datetime.now()
        # 每天晚上十一點更新
        if now.hour == 23:
            log.d("Today is ", now)
            if thr is not None and thr.is_alive():
                log.e("update_stock_info_service is still running, something wrong.")
            else:
                thr = threading.Thread(target=update_stock_info_service.update_daily_data, args=(updated_cb, now))
                thr.setDaemon(True)
                thr.start()

        time.sleep(600)
        log.d("Just show a useless log ...")