import sys
import os
sys.path.append(
    os.path.abspath(
        os.path.join(
            os.path.abspath(
                os.path.dirname(__file__)),
            '..')))

import config
from twisted.application import service, internet
from graphite import datalib
from db import Db
from api.site import Site

topService = service.MultiService()

db = Db()
datalib.db = db
db.setServiceParent(topService)

httpService = internet.TCPServer(config.HTTP_PORT, Site(db), interface=config.HTTP_ADDR)
httpService.setServiceParent(topService)

application = service.Application("moira-worker-api")
topService.setServiceParent(application)
