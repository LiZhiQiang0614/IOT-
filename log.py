# -*- coding: utf-8 -*-
import os
import time
import logging
import inspect
# from logging.handlers import RotatingFileHandler
from logging.handlers import TimedRotatingFileHandler

dir = os.path.dirname(__file__)
dir_time = time.strftime('%Y-%m-%d-%S', time.localtime())

handlers = {  # logging.NOTSET: os.path.join(dir, 'notset_%s.log'%dir_time),

    logging.DEBUG: os.path.join(dir, 'logs/debug/debug.log'),

    logging.INFO: os.path.join(dir, 'logs/info/info.log'),

    logging.WARNING: os.path.join(dir, 'logs/warning/logs.log'),

    logging.ERROR: os.path.join(dir, 'logs/error/error.log'),

    # logging.CRITICAL: os.path.join(dir, 'critical_%s.log'%dir_time),
}


def createHandlers():
    logLevels = handlers.keys()
    debug_path = os.path.join(dir, 'logs/debug')
    info_path = os.path.join(dir, 'logs/info')
    error_path = os.path.join(dir, 'logs/error')
    warning_path = os.path.join(dir, 'logs/warning')
    is_exists_debug = os.path.exists(debug_path)
    is_exists_info = os.path.exists(info_path)
    is_exists_error = os.path.exists(error_path)
    is_exists_warning = os.path.exists(warning_path)
    if not is_exists_debug:
        os.makedirs(debug_path)
    if not is_exists_info:
        os.makedirs(info_path)
    if not is_exists_error:
        os.makedirs(error_path)
    if not is_exists_warning:
        os.makedirs(warning_path)
    for level in logLevels:
        path = os.path.abspath(handlers[level])
        # handlers[level] = RotatingFileHandler(path, maxBytes=1024 * 10000, backupCount=2, encoding='utf-8')
        handlers[level] = TimedRotatingFileHandler(filename=path, when='D', interval=1, backupCount=0)


# 加载模块时创建全局变量

createHandlers()


class TNLog(object):

    def printfNow(self):
        return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

    def __init__(self, level=logging.NOTSET):
        self.__loggers = {}

        logLevels = handlers.keys()

        for level in logLevels:
            logger = logging.getLogger(str(level))

            # 如果不指定level，获得的handler似乎是同一个handler

            logger.addHandler(handlers[level])

            logger.setLevel(level)

            self.__loggers.update({level: logger})

    def getLogMessage(self, level, message):
        frame, filename, lineNo, functionName, code, unknowField = inspect.stack()[2]

        '''日志格式：[时间] [类型] [记录代码] 信息'''

        return "[%s] [%s] [%s - %s - %s] %s" % (self.printfNow(), level, filename, lineNo, functionName, message)

    def info(self, message):
        message = self.getLogMessage("info", message)

        self.__loggers[logging.INFO].info(message)

    def error(self, message):
        message = self.getLogMessage("error", message)

        self.__loggers[logging.ERROR].error(message)

    def warning(self, message):
        message = self.getLogMessage("warning", message)

        self.__loggers[logging.WARNING].warning(message)

    def debug(self, message):
        message = self.getLogMessage("debug", message)

        self.__loggers[logging.DEBUG].debug(message)

    def critical(self, message):
        message = self.getLogMessage("critical", message)

        self.__loggers[logging.CRITICAL].critical(message)


if __name__ == "__main__":
    pass
    # logger = TNLog()
    # logger.debug("debug")
    # time.sleep(10)
    # logger.info("info")
    # logger.warning("warning")
    # logger.error("error")
