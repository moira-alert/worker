from twisted.application import service, internet
from twisted.internet import reactor
from moira import config
from moira.graphite import datalib
from moira.db import Db
from moira.api.site import Site
from moira import logs


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
