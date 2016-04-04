import sys
import os
sys.path.insert(0,
                os.path.abspath(
                    os.path.join(
                        os.path.abspath(
                            os.path.dirname(__file__)),
                        '../../')))
from fakeredis import FakeStrictRedis, FakePipeline
from StringIO import StringIO
from twisted.trial import unittest
from twisted.python import log
from twisted.web import client
from twisted.internet import reactor, protocol
from twisted.internet.defer import Deferred, inlineCallbacks
from moira.api.site import Site
from moira.checker.master import MasterProtocol
from moira.graphite import datalib
from moira.checker.worker import TriggersCheck
from moira.checker.trigger import Trigger
from moira import db
from moira import config


def trigger(trigger_id):
    def decorator(f):
        def wrapper(*args, **kwargs):
            worker_test = args[0]
            worker_test.trigger = Trigger(trigger_id, worker_test.db)
            return f(*args, **kwargs)
        return wrapper
    return decorator


class TwistedFakeTransaction():

    def __init__(self, pipeline):
        self.pipeline = pipeline

    def __getattr__(self, name):
        return self.pipeline.__getattr__(name)

    def __enter__(self):
        return self.pipeline

    def __exit__(self, exc_type, exc_value, traceback):
        self.pipeline.reset()

    def commit(self):
        return self.pipeline.execute()


class TwistedFakePipeline(FakePipeline):

    def __init__(self, owner, transaction=True):
        super(TwistedFakePipeline, self).__init__(owner, transaction)

    def execute_pipeline(self):
        return FakePipeline.execute(self)


class TwistedFakeRedis(FakeStrictRedis):

    def __init__(self):
        super(TwistedFakeRedis, self).__init__()

    def zrange(self, key, start=0, end=-1, withscores=False):
        return FakeStrictRedis.zrange(self, key, start, end, withscores=withscores)

    def zrangebyscore(self, key, min='-inf', max='+inf',
                      withscores=False, offset=None, count=None):
        return FakeStrictRedis.zrangebyscore(self, key, min, max, start=offset, num=count, withscores=withscores)

    def multi(self):
        return TwistedFakeTransaction(self.pipeline(transaction=True))

    def disconnect(self):
        pass

    def pipeline(self, transaction=True):
        return TwistedFakePipeline(self, transaction)

    def getset(self, name, value):
        val = self._db.get(name)
        self._db[name] = value
        return val

    def set(self, key, value, expire=None, pexpire=None,
            only_if_not_exists=False, only_if_exists=False):
        return FakeStrictRedis.set(self, key, value, ex=expire, px=pexpire, nx=only_if_not_exists,
                                   xx=only_if_exists)

class BodyReceiver(protocol.Protocol):

    def __init__(self):
        self.finished = Deferred()
        self.content = StringIO()

    def dataReceived(self, bytes):
        self.content.write(bytes)

    def connectionLost(self, reason):
        self.finished.callback(self.content.getvalue())


class WorkerTests(unittest.TestCase):

    @inlineCallbacks
    def setUp(self):
        log.startLogging(sys.stdout)
        self.db = db.Db()
        self.db.rc = TwistedFakeRedis()
        yield self.db.startService()
        yield self.db.flush()
        datalib.db = self.db
        site = Site(self.db)
        self.protocol = MasterProtocol()
        self.protocol.factory = self
        self.port = reactor.listenTCP(0, site, interface="127.0.0.1")
        self.client = client.Agent(reactor)
        self.url_prefix = 'http://localhost:{0}{1}/'.format(
            self.port.getHost().port, site.prefix)
        self.now = int(reactor.seconds())
        self.check = TriggersCheck(self.db)

    @inlineCallbacks
    def tearDown(self):
        yield self.db.stopService()
        yield self.port.stopListening()

    @inlineCallbacks
    def assert_trigger_metric(self, metric, value, state):
        check = yield self.db.getTriggerLastCheck(self.trigger.id)
        log.msg("Received check: %s" % check)
        self.assertIsNot(check, None)
        metric = [m for m in check["metrics"].itervalues()][0] \
            if isinstance(metric, int) \
            else check["metrics"].get(metric, {})
        self.assertEquals(value, metric.get("value"))
        self.assertEquals(state, metric.get("state"))
