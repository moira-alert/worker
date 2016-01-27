import sys
import txredisapi
from twisted.python import log
from twisted.internet import defer, reactor
from moira.db import Db, METRIC_OLD_PREFIX, METRIC_PREFIX
from moira import config


@defer.inlineCallbacks
def convert(db):

    log.msg(db.rc)
    log.msg("Reading metrics keys")
    keys = yield db.rc.keys(METRIC_OLD_PREFIX.format("*"))
    log.msg("Converting ...")
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
            log.err("Can not convert %s: %s" % (key, e))
        log.msg("Metric {0} converted".format(name))

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
