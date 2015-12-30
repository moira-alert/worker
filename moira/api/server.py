from moira import config
from twisted.application import service, internet
from moira.graphite import datalib
from moira.db import Db
from moira.api.site import Site

topService = service.MultiService()

db = Db()
datalib.db = db
db.setServiceParent(topService)

httpService = internet.TCPServer(config.HTTP_PORT, Site(db), interface=config.HTTP_ADDR)
httpService.setServiceParent(topService)

application = service.Application("moira-worker-api")
topService.setServiceParent(application)
