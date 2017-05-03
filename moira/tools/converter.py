import sys

import txredisapi
from twisted.internet import defer, reactor

from moira.logs import log
from moira import config
from moira.db import Db, METRIC_OLD_PREFIX, METRIC_PREFIX


@defer.inlineCallbacks
def convert(db):

    log.info(db.rc)
    log.info("Reading metrics keys")
    keys = yield db.rc.keys(METRIC_OLD_PREFIX.format("*"))
    log.info("Converting ...")
    for key in keys:
        _, name = key.split(':')
        try:
            pipe = yield db.rc.pipeline()
            metrics = yield db.rc.zrange(key)
            for metric in metrics:
                value, timestamp = metric.split()
                pipe.zadd(METRIC_PREFIX.format(name), timestamp, "{0} {1}".format(timestamp, value))
            yield pipe.execute_pipeline()
        except txredisapi.ResponseError as e:
            log.error("Can not convert {key}: {e}", key=key, e=e)
        log.info("Metric {name} converted", name=name)

    yield db.stopService()
    reactor.stop()


def run():

    config.read()
    config.LOG_DIRECTORY = "stdout"
    log.startLogging(sys.stdout)

    db = Db()
    db.startService().addCallback(convert)

    reactor.run()


if __name__ == '__main__':

    run()
