import socket
import anyjson
import yaml
import os

try:
    import ujson
    ujson.loads("{}")
    if anyjson._modules[0][0] != 'ujson':
        anyjson._modules.insert(
            0,
            ("ujson",
             "dumps",
             TypeError,
             "loads",
             ValueError,
             "load"))
    anyjson.force_implementation('ujson')
except ImportError:
    pass

CONFIG_PATH = '/etc/moira/config.yml'
REDIS_HOST = "localhost"
REDIS_PORT = 6379
LOG_DIRECTORY = "log/worker"
HTTP_PORT = 8081
HTTP_ADDR = ''
GRAPHITE = []
GRAPHITE_PREFIX = 'DevOps.moira'
GRAPHITE_INTERVAL = 10
NODATA_CHECK_INTERVAL = 60
CHECK_INTERVAL = 5
METRICS_TTL = 3600
PREFIX = "/api"

if os.path.exists(CONFIG_PATH):
    with open(CONFIG_PATH, 'r') as yml:
        cfg = yaml.load(yml)
        REDIS_HOST = cfg['redis']['host']
        REDIS_PORT = cfg['redis']['port']
        LOG_DIRECTORY = cfg['worker']['log_dir']
        HTTP_PORT = cfg['api']['port']
        HTTP_ADDR = cfg['api']['listen']
        for key in cfg['graphite']:
            if key.startswith('uri'):
                host, port = cfg['graphite'][key].split(':')
                GRAPHITE.append((host, int(port)))
        GRAPHITE_PREFIX = cfg['graphite']['prefix']
        GRAPHITE_INTERVAL = cfg['graphite']['interval']
        NODATA_CHECK_INTERVAL = cfg['checker'].get('nodata_check_interval', 60)
        CHECK_INTERVAL = cfg['checker'].get('check_interval', 5)
        METRICS_TTL = cfg['checker'].get('metrics_ttl', 3600)


HOSTNAME = socket.gethostname().split('.')[0]
