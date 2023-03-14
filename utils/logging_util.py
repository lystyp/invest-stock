# -*- coding: UTF-8 -*-
import logging
import logging.handlers
import os
import threading
import re

# Avoid generating same logger
count = 0

MAX_LOG_SIZE = 50 * 1024 * 1024 
LOG_PATH = os.path.abspath('.') + "/logs/"

class Logger:
    def __init__(self, tag=None):
        global count
        count += 1

        self.logger = logging.getLogger(str(count))
        self.logger.setLevel('DEBUG')
        self.tag = tag

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s\t : %(message)s'))

        # Windows系統不支援RotatingFileHandler做log file切換，所以log會無限制增長下去
        if not os.path.exists(LOG_PATH):
            os.mkdir(LOG_PATH)
        file_handler = None
        if os.name == 'nt':
            file_handler = logging.FileHandler(filename=LOG_PATH + "invest-stock-log.log", encoding='utf-8')
        else:
            file_handler = NewRotatingFileHandler(filename=LOG_PATH + "invest-stock-log.log", encoding='utf-8', maxBytes=MAX_LOG_SIZE, backupCount=1)
        file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s\t : %(message)s'))

        self.logger.addHandler(stream_handler)
        self.logger.addHandler(file_handler)

    def set_tag(self, tag):
        self.tag = tag
   
    def d(self, *args):
        s = ""
        for arg in args:
            s = s + str(arg)
        s = s if self.tag is None else self.tag + " - " + s
        self.logger.debug(s)

    def i(self, *args):
        s = ""
        for arg in args:
            s = s + str(arg)
        s = s if self.tag is None else self.tag + " - " + s
        self.logger.info(s)

    def w(self, *args):
        s = ""
        for arg in args:
            s = s + str(arg)
        s = s if self.tag is None else self.tag + " - " + s
        self.logger.warn(s)

    def e(self, *args):
        s = ""
        for arg in args:
            s = s + str(arg)
        s = s if self.tag is None else self.tag + " - " + s
        self.logger.error(s)

class NewRotatingFileHandler(logging.handlers.RotatingFileHandler):
    def doRollover(self):
        super().doRollover()
        thr = threading.Thread(target=self.__rename_log)
        thr.setDaemon(True)
        thr.start()
        
    def __rename_log(self):
        for name in os.listdir(LOG_PATH):
            l = name.split(".")
            if len(l) == 3 and l[0] == "invest-stock-log" and l[1] == "log":
                with open(LOG_PATH + name, "r") as file:
                    lines = file.readlines()
                    begin_time = ""
                    end_time = ""
                    for i in lines:
                        s = i.split(" ")
                        if len(s) > 2:
                            # replace space with dash.
                            t = s[0] + "-" + s[1]
                            if re.match (r'\d+-\d+-\d+-\d+:\d+:\d+,\d+', t) is not None:
                                begin_time = t
                                break

                    for i in reversed(lines):
                        s = i.split(" ")
                        if len(s) > 2:
                            # replace space with dash.
                            t = s[0] + "-" + s[1]
                            if re.match (r'\d+-\d+-\d+-\d+:\d+:\d+,\d+', t) is not None:
                                end_time = t 
                                break
                os.rename(LOG_PATH + name, LOG_PATH + "invest-stock-log" + "_" + begin_time + "_" + end_time + ".log")
    

if __name__ == '__main__':
    pass

