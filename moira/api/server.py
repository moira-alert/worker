from moira.graphite import datalib
from twisted.application import service, internet
from twisted.internet import reactor

from moira import config
from moira import logs
from moira.api.site import Site
from moira.db import Db


def run():

    config.read()
    logs.api()

    top_service = service.MultiService()

    db = Db()
    datalib.db = db
    db.setServiceParent(top_service)

    http_service = internet.TCPServer(config.HTTP_PORT, Site(db), interface=config.HTTP_ADDR)
    http_service.setServiceParent(top_service)

    top_service.startService()

    reactor.addSystemEventTrigger('before', 'shutdown', top_service.stopService)

    reactor.run()
