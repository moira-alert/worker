import time
from moira import config
from moira.logs import log
from twisted.internet import reactor
from twisted.internet.endpoints import TCP4ClientEndpoint
from twisted.internet.protocol import Factory, Protocol
from twisted.internet.task import LoopingCall


class GraphiteProtocol(Protocol):

    def send_metrics(self, get_metrics):
        timestamp = int(time.time())
        metrics = get_metrics()
        for name, value in metrics:
            self.transport.write(
                "%s.%s %s %s\n" %
                (config.GRAPHITE_PREFIX, name, value, timestamp))

    def connectionLost(self, reason):
        log.error(str(reason))
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
        end_point = TCP4ClientEndpoint(reactor, self.host, self.port, 10)
        d = end_point.connect(Factory.forProtocol(GraphiteProtocol))

        def success(connection):
            self.connecting = False
            log.info('Connected to {replica}', replica=self)
            self.connection = connection

        def failed(error):
            log.error('Connect to {replica} failed: {error}', replica=self, error=error)
            reactor.callLater(10, self.connect, True)
        d.addCallbacks(success, failed)

    def connected(self):
        return self.connection and self.connection.connected

    def send(self, get_metrics):
        self.connection.send_metrics(get_metrics)


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
                log.error("No graphite connection")
                return
            replica = self.replicas[self.index]
        replica.send(get_metrics)
        self.next()
        log.info("Sent metrics to {replica}", replica=replica)


def sending(get_metrics):
    if not config.GRAPHITE:
        return
    client = GraphiteClusterClient(
        [GraphiteReplica(host, port) for host, port in config.GRAPHITE])
    client.connect()
    lc = LoopingCall(client.send, get_metrics)
    lc.start(config.GRAPHITE_INTERVAL, now=False)
