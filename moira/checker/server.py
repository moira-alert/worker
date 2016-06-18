import multiprocessing
import os
import sys

from moira.graphite import datalib
from twisted.application import service
from twisted.internet import reactor
from twisted.internet.protocol import ProcessProtocol

from moira import config
from moira import logs
from moira.logs import log
from moira.checker.master import MasterService
from moira.checker.worker import check
from moira.db import Db

WORKER_PATH = os.path.abspath(
    os.path.join(
        os.path.abspath(
            os.path.dirname(__file__)), 'worker.py'))


class CheckerProcessProtocol(ProcessProtocol):

    def connectionMade(self):
        log.info("Run worker - {pid}", pid=self.transport.pid)

    def processEnded(self, reason):
        log.info("Checker process ended with reason: {reason}", reason=reason)
        if reactor.running:
            reactor.stop()


class TopService(service.MultiService):

    checkers = []

    def startService(self):
        service.MultiService.startService(self)
        for i in range(max(1, multiprocessing.cpu_count() - 1)):
            checker = reactor.spawnProcess(
                CheckerProcessProtocol(), sys.executable,
                ['moira-checker', WORKER_PATH, "-n", str(i), "-c", config.CONFIG_PATH, "-l", config.LOG_DIRECTORY],
                childFDs={0: 'w', 1: 1, 2: 2}, env=os.environ)
            self.checkers.append(checker)


def run():

    config.read()
    logs.checker_master()

    if config.ARGS.t:
        check(config.ARGS.t)
        return

    top_service = TopService()

    db = Db()
    datalib.db = db
    db.setServiceParent(top_service)

    sub_service = MasterService(db)
    sub_service.setServiceParent(top_service)

    top_service.startService()

    reactor.addSystemEventTrigger('before', 'shutdown', top_service.stopService)

    reactor.run()
