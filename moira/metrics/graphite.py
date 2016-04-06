import time
from moira import config
from twisted.internet import reactor
from twisted.internet.endpoints import TCP4ClientEndpoint
from twisted.internet.protocol import Factory, Protocol
from twisted.internet.task import LoopingCall
from twisted.python import log


class GraphiteProtocol(Protocol):

    def sendMetrics(self, get_metrics):
        timestamp = int(time.time())
        metrics = get_metrics()
        for name, value in metrics:
            self.transport.write(
                "%s.%s %s %s\n" %
                (config.GRAPHITE_PREFIX, name, value, timestamp))

    def connectionLost(self, reason):
        log.err(str(reason))
        self.connected = 0


class GraphiteReplica(object):

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.connection = None
        self.connecting = False

    def __str__(self):
        return "%s:%s" % (self.host, self.port)

    def connect(self, reconnecting=False):
        if self.connecting and not reconnecting:
            return
        self.connecting = True
        endPoint = TCP4ClientEndpoint(reactor, self.host, self.port, 10)
        d = endPoint.connect(Factory.forProtocol(GraphiteProtocol))

        def success(connection):
            self.connecting = False
            log.msg('Connected to %s' % self)
            self.connection = connection

        def failed(error):
            log.err('Connect to %s failed: %s' % (self, error))
            reactor.callLater(10, self.connect, True)
        d.addCallbacks(success, failed)

    def connected(self):
        return self.connection and self.connection.connected

    def send(self, get_metrics):
        self.connection.sendMetrics(get_metrics)


class GraphiteClusterClient(object):

    def __init__(self, replicas):
        self.replicas = replicas
        self.index = 0

    def connect(self):
        for replica in self.replicas:
            replica.connect()

    def next(self):
        self.index = (self.index + 1) % len(self.replicas)

    def send(self, get_metrics):
        index = self.index
        replica = self.replicas[self.index]
        while not replica.connected():
            replica.connect()
            self.next()
            if self.index == index:
                log.err("No graphite connection")
                return
            replica = self.replicas[self.index]
        replica.send(get_metrics)
        self.next()
        log.msg("Sent metrics to %s" % replica)


def sending(get_metrics):
    if not config.GRAPHITE:
        return
    client = GraphiteClusterClient(
        [GraphiteReplica(host, port) for host, port in config.GRAPHITE])
    client.connect()
    lc = LoopingCall(client.send, get_metrics)
    lc.start(config.GRAPHITE_INTERVAL, now=False)
