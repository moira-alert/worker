import os
import sys
from twisted.python import log
from twisted.python.log import FileLogObserver
from twisted.python.logfile import DailyLogFile
from moira import config


def api():
    if config.LOG_DIRECTORY == "stdout":
        log.startLogging(sys.stdout)
    else:
        log.startLogging(FileLogObserver(daily("api.log")))


def checker_master():
    if config.LOG_DIRECTORY == "stdout":
        log.startLogging(sys.stdout)
    else:
        log.startLogging(FileLogObserver(daily("checker.log")))


def checker_worker():
    if config.LOG_DIRECTORY == "stdout":
        log.startLogging(sys.stdout)
    else:
        log.startLogging(FileLogObserver(daily("checker-{0}.log".format(config.ARGS.n))))


def daily(name):
    path = os.path.abspath(config.LOG_DIRECTORY)
    if not os.path.exists(path):
        os.makedirs(path)
    return DailyLogFile(name, path)
