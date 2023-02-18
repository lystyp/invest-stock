# -*- coding: UTF-8 -*-
import logging
import time

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
        file_handler = logging.FileHandler(filename="invest-stock-log.log", encoding='utf-8')
        file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s\t : %(message)s'))

        self.logger.addHandler(stream_handler)
        self.logger.addHandler(file_handler)

    def set_tag(self, tag):
        self.tag = tag
   
    def d(self, message = None):
        s = str(message) if self.tag is None else self.tag + " - " + str(message)
        self.logger.debug(s)

    def i(self, message = None):
        s = str(message) if self.tag is None else self.tag + " - " + str(message)
        self.logger.info(s)

    def w(self, message = None):
        s = str(message) if self.tag is None else self.tag + " - " + str(message)
        self.logger.warn(s)

    def e(self, message = None):
        s = str(message) if self.tag is None else self.tag + " - " + str(message)
        self.logger.error(s)


    

if __name__ == '__main__':
    pass

