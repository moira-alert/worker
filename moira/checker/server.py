import multiprocessing
import os
from sys import executable
from os import environ
from twisted.application import service
from twisted.python import log
from twisted.internet import reactor
from twisted.internet.protocol import ProcessProtocol
from moira.checker.master import MasterService
from moira.graphite import datalib
from moira.db import Db
from moira import config
from moira import logs

WORKER_PATH = os.path.abspath(
    os.path.join(
        os.path.abspath(
            os.path.dirname(__file__)), 'worker.py'))


class CheckerProcessProtocol(ProcessProtocol):

    def connectionMade(self):
        log.msg("Run worker - %s" % self.transport.pid)

    def processEnded(self, reason):
        log.msg("Checker process ended with reason: %s" % reason)
        if reactor.running:
            reactor.stop()


class TopService(service.MultiService):

    checkers = []

    def startService(self):
        service.MultiService.startService(self)
        for i in range(max(1, multiprocessing.cpu_count() - 1)):
            checker = reactor.spawnProcess(
                CheckerProcessProtocol(), executable,
                ['moira-checker', WORKER_PATH, "-n", str(i), "-c", config.CONFIG_PATH, "-l", config.LOG_DIRECTORY],
                childFDs={0: 'w', 1: 1, 2: 2}, env=environ)
            self.checkers.append(checker)


def run():

    config.read()
    logs.checker_master()

    topService = TopService()

    db = Db()
    datalib.db = db
    db.setServiceParent(topService)

    subService = MasterService(db)
    subService.setServiceParent(topService)

    topService.startService()

    reactor.addSystemEventTrigger('before', 'shutdown', topService.stopService)

    reactor.run()
