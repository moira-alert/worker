import os
import sys

from twisted.logger import Logger
from twisted.logger import globalLogPublisher
from twisted.logger import textFileLogObserver
from twisted.logger import FilteringLogObserver, LogLevelFilterPredicate, LogLevel
from twisted.python.logfile import DailyLogFile

from moira import config

log = Logger()

levels = {
    'debug': LogLevel.debug,
    'info': LogLevel.info,
    'warn': LogLevel.warn,
    'warning': LogLevel.warn,
    'error': LogLevel.error,
    'critical': LogLevel.critical
}


class ZeroPaddingDailyLogFile(DailyLogFile):

    def suffix(self, tupledate):
        """Return the suffix given a (year, month, day) tuple or unixtime"""
        try:
            return ''.join(map(lambda x: ("0%d" % x) if x < 10 else str(x), tupledate))
        except Exception:
            # try taking a float unixtime
            return ''.join(map(str, self.toDate(tupledate)))


def init(outFile):
    level = levels[config.LOG_LEVEL]
    predicate = LogLevelFilterPredicate(defaultLogLevel=level)
    observer = FilteringLogObserver(textFileLogObserver(outFile=outFile), [predicate])
    observer._encoding = "utf-8"
    globalLogPublisher.addObserver(observer)
    log.info("Start logging with {l}", l=level)


def api():
    init(sys.stdout if config.LOG_DIRECTORY == "stdout" else daily("api.log"))


def checker_master():
    outFile = sys.stdout if config.LOG_DIRECTORY == "stdout" else daily("checker.log")
    init(outFile)


def checker_worker():
    outFile = sys.stdout if config.LOG_DIRECTORY == "stdout" else daily("checker-{0}.log".format(config.ARGS.n))
    init(outFile)


def audit():
    outFile = sys.stdout if config.LOG_DIRECTORY == "stdout" else daily("audit.log")
    observer = textFileLogObserver(outFile=outFile)
    observer._encoding = "utf-8"
    return Logger(observer=observer)


def daily(name):
    path = os.path.abspath(config.LOG_DIRECTORY)
    if not os.path.exists(path):
        os.makedirs(path)
    return ZeroPaddingDailyLogFile(name, path)
