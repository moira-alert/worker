import sys
import os
sys.path.append(
    os.path.abspath(
        os.path.join(
            os.path.abspath(
                os.path.dirname(__file__)),
            '../../bin')))

from twisted.trial import unittest
from twisted.python import log
from twisted.web import client
from twisted.internet import reactor, protocol
from twisted.internet.defer import Deferred, inlineCallbacks
from StringIO import StringIO
from api.site import Site
from checker.subscriber import SubscriberProtocol
from graphite import datalib
from checker.check import TriggersCheck
from fakeredis import FakeStrictRedis, FakePipeline
import db


def trigger(trigger_id):
    def decorator(f):
        def wrapper(*args, **kwargs):
            args[0].trigger_id = trigger_id
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
        self.protocol = SubscriberProtocol()
        self.protocol.factory = self
        self.port = reactor.listenTCP(0, site, interface="127.0.0.1")
        self.client = client.Agent(reactor)
        self.url_prefix = 'http://localhost:{0}{1}/'.format(
            self.port.getHost().port, site.prefix)
        self.now = int(reactor.seconds())
        self.trigger_id = None
        self.check = TriggersCheck(self.db)

    @inlineCallbacks
    def tearDown(self):
        yield self.db.stopService()
        yield self.port.stopListening()
