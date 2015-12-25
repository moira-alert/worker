import anyjson
import txredisapi as redis
import config
from twisted.application import service
from twisted.python import log
from twisted.internet import defer, reactor
from twisted.internet.task import LoopingCall


class SubscriberProtocol(redis.SubscriberProtocol):

    @defer.inlineCallbacks
    def messageReceived(self, ignored, channel, message, nocache=False):
        try:
            json = anyjson.deserialize(message)
            db = self.factory.db
            pattern = json["pattern"]
            metric = json["metric"]
            yield db.addPatternMetric(pattern, metric)
            triggers = yield db.getPatternTriggers(pattern)
            if len(triggers) == 0:
                yield db.removePatternTriggers(pattern)
                yield db.removePattern(pattern)
                metrics = yield db.getPatternMetrics(pattern)
                for metric in metrics:
                    yield db.delMetric(metric)
                yield db.delPatternMetrics(pattern)

            for trigger_id in triggers:
                if nocache:
                    yield db.addTriggerCheck(trigger_id)
                else:
                    yield db.addTriggerCheck(trigger_id, cache_key=trigger_id, cache_ttl=config.CHECK_INTERVAL)
        except:
            log.err()


class SubscriberService(service.Service):

    def __init__(self, db, channel="metric-event"):
        self.db = db
        self.channel = channel

    @defer.inlineCallbacks
    def startService(self):
        service.Service.startService(self)
        factory = redis.SubscriberFactory()
        factory.protocol = SubscriberProtocol
        factory.continueTrying = True
        factory.db = self.db
        reactor.connectTCP(config.REDIS_HOST, config.REDIS_PORT, factory)
        self.rc = yield factory.deferred
        yield self.rc.subscribe(self.channel)
        log.msg('Subscribed to %s' % self.channel)
        self.lc = LoopingCall(self.checkNoData)
        self.nodata_check = self.lc.start(config.NODATA_CHECK_INTERVAL, now=True)

    @defer.inlineCallbacks
    def checkNoData(self):
        try:
            log.msg("Checking nodata")
            triggers = yield self.db.getTriggers()
            for trigger_id in triggers:
                yield self.db.addTriggerCheck(trigger_id, cache_key=trigger_id, cache_ttl=60)
        except:
            log.err()

    @defer.inlineCallbacks
    def stopService(self):
        yield self.lc.stop()
        yield self.nodata_check
        yield self.rc.disconnect()
