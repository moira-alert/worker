from twisted.python.log import FileLogObserver
from twisted.python.logfile import DailyLogFile
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.abspath(
    os.path.dirname(__file__)), '..')))

import config


def api():
    return FileLogObserver(daily("api.log")).emit


def checker():
    return FileLogObserver(daily("checker.log")).emit


def daily(name):
    path = os.path.abspath(config.LOG_DIRECTORY)
    if not os.path.exists(path):
        os.makedirs(path)
    return DailyLogFile(name, path)


def reformat_trigger(trigger, trigger_id, tags):
    if trigger_id:
        trigger["id"] = trigger_id
    trigger["tags"] = list(tags)
    trigger["warn_value"] = float(trigger["warn_value"])
    trigger["error_value"] = float(trigger["error_value"])
    ttl = trigger.get("ttl")
    if ttl:
        trigger["ttl"] = int(ttl)
    else:
        trigger["ttl"] = None
    return trigger
