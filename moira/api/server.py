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

    topService = service.MultiService()

    db = Db()
    datalib.db = db
    db.setServiceParent(topService)

    httpService = internet.TCPServer(config.HTTP_PORT, Site(db), interface=config.HTTP_ADDR)
    httpService.setServiceParent(topService)

    topService.startService()

    reactor.addSystemEventTrigger('before', 'shutdown', topService.stopService)

    reactor.run()
