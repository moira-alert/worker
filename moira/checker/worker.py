import random
from twisted.python import log
from twisted.internet import defer, reactor, task
from moira.graphite import datalib
from moira.metrics import spy, graphite
from moira.db import Db
from moira import logs
from moira import config
from moira.checker.trigger import Trigger


PERFORM_INTERVAL = 0.01
ERROR_TIMEOUT = 10


class TriggersCheck:

    def __init__(self, db):
        self.db = db

    def start(self):
        self.t = task.LoopingCall(self.perform)
        self.finished = self.t.start(PERFORM_INTERVAL, now=False)
        log.msg("Checker service started")

    @defer.inlineCallbacks
    def stop(self):
        self.t.stop()
        yield self.finished

    @defer.inlineCallbacks
    def perform(self):
        try:
            trigger_id = yield self.db.getTriggerToCheck()
            while trigger_id is not None:
                start = reactor.seconds()
                trigger = Trigger(trigger_id, self.db)
                yield trigger.check()
                end = reactor.seconds()
                spy.TRIGGER_CHECK.report(end - start)
                trigger_id = yield self.db.getTriggerToCheck()
            yield task.deferLater(reactor, random.uniform(PERFORM_INTERVAL * 10, PERFORM_INTERVAL * 20), lambda: None)
        except GeneratorExit:
            pass
        except:
            spy.TRIGGER_CHECK_ERRORS.report(0)
            log.err()
            yield task.deferLater(reactor, ERROR_TIMEOUT, lambda: None)


def run(callback):

    db = Db()
    datalib.db = db
    init = db.startService()
    init.addCallback(callback)

    reactor.run()


def main(number):

    def get_metrics():
        return [
            ("checker.time.%s.%s" %
             (config.HOSTNAME,
              number),
                spy.TRIGGER_CHECK.get_metrics()["sum"]),
            ("checker.triggers.%s.%s" %
             (config.HOSTNAME,
              number),
                spy.TRIGGER_CHECK.get_metrics()["count"]),
            ("checker.errors.%s.%s" %
             (config.HOSTNAME,
              number),
                spy.TRIGGER_CHECK_ERRORS.get_metrics()["count"])]

    graphite.sending(get_metrics)

    def start(db):
        checker = TriggersCheck(db)
        checker.start()
        reactor.addSystemEventTrigger('before', 'shutdown', checker.stop)

    run(start)


def check(trigger_id):

    @defer.inlineCallbacks
    def start(db):
        trigger = Trigger(trigger_id, db)
        yield trigger.check(db, trigger_id)
        reactor.stop()

    run(start)


if __name__ == '__main__':

    config.read()
    logs.checker_worker()

    if config.ARGS.t:
        check(config.ARGS.t)
    else:
        main(config.ARGS.n)
