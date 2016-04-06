import anyjson
import txredisapi as redis
from twisted.application import service
from twisted.python import log
from twisted.internet import defer, reactor
from twisted.internet.task import LoopingCall
from moira import config


class MasterProtocol(redis.SubscriberProtocol):

    @defer.inlineCallbacks
    def messageReceived(self, ignored, channel, message, nocache=False):
        try:
            json = anyjson.deserialize(message)
            db = self.factory.db
            db.last_data = reactor.seconds()
            pattern = json["pattern"]
            metric = json["metric"]
            yield db.addPatternMetric(pattern, metric)
            triggers = yield db.getPatternTriggers(pattern)
            if len(triggers) == 0:
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
        except Exception:
            log.err()


class MasterService(service.Service):

    def __init__(self, db, channel="metric-event"):
        self.db = db
        self.channel = channel
        self.db.last_data = reactor.seconds()

    @defer.inlineCallbacks
    def startService(self):
        service.Service.startService(self)
        factory = redis.SubscriberFactory()
        factory.protocol = MasterProtocol
        factory.continueTrying = True
        factory.db = self.db
        yield self.db.startService()
        yield reactor.connectTCP(config.REDIS_HOST, config.REDIS_PORT, factory)
        self.rc = yield factory.deferred
        yield self.rc.subscribe(self.channel)
        log.msg('Subscribed to %s' % self.channel)
        self.lc = LoopingCall(self.checkNoData)
        self.nodata_check = self.lc.start(config.NODATA_CHECK_INTERVAL, now=True)

    @defer.inlineCallbacks
    def checkNoData(self):
        try:
            now = reactor.seconds()
            if self.db.last_data + config.STOP_CHECKING_INTERVAL < now:
                log.msg("Checking nodata disabled. No metrics for %s seconds" % int(now - self.db.last_data))
            else:
                log.msg("Checking nodata")
                triggers = yield self.db.getTriggers()
                for trigger_id in triggers:
                    yield self.db.addTriggerCheck(trigger_id, cache_key=trigger_id, cache_ttl=60)
        except Exception:
            log.err()

    @defer.inlineCallbacks
    def stopService(self):
        yield self.lc.stop()
        yield self.nodata_check
        yield self.rc.disconnect()
