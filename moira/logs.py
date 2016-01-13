from twisted.python.log import FileLogObserver
from twisted.python.logfile import DailyLogFile
from moira import config
import os


def api():
    return FileLogObserver(daily("api.log"))


def checker():
    return FileLogObserver(daily("checker.log"))


def daily(name):
    path = os.path.abspath(config.LOG_DIRECTORY)
    if not os.path.exists(path):
        os.makedirs(path)
    return DailyLogFile(name, path)
