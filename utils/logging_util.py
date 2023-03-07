# -*- coding: UTF-8 -*-
import logging
import time
import os

# Avoid generating same logger
count = 0

class Logger:
    def __init__(self, tag=None) -> None:
        global count
        count += 1

        self.logger = logging.getLogger(str(count))
        self.logger.setLevel('DEBUG')
        self.tag = tag

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s\t : %(message)s'))
        file_handler = logging.FileHandler(filename=os.path.abspath('.') + "/logs/invest-stock-log.log", encoding='utf-8')
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


    

if __name__ == '__main__':
    pass

