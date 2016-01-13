import sys
from twisted.application import service, internet
from twisted.internet import reactor
from twisted.python import log
from moira import config
from moira.graphite import datalib
from moira.db import Db
from moira.api.site import Site
from moira import logs


def run():

    parser = config.get_parser()
    parser.add_argument('-port', help='listening port', default=8081, type=int)
    args = parser.parse_args()

    config.CONFIG_PATH = args.c
    config.HTTP_PORT = args.port
    config.LOG_DIRECTORY = args.l

    logger = logs.api() if args.l != "stdout" else sys.stdout
    log.startLogging(logger)

    topService = service.MultiService()

    db = Db()
    datalib.db = db
    db.setServiceParent(topService)

    httpService = internet.TCPServer(config.HTTP_PORT, Site(db), interface=config.HTTP_ADDR)
    httpService.setServiceParent(topService)

    topService.startService()

    reactor.addSystemEventTrigger('before', 'shutdown', topService.stopService)

    reactor.run()
