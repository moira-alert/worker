from functools import wraps

from twisted.internet import reactor, defer

CACHE = {}


def cache(f):
    @wraps(f)
    @defer.inlineCallbacks
    def wrapper(*args, **kwargs):
        if 'cache_key' in kwargs and 'cache_ttl' in kwargs:
            key = "%s%s" % (f, kwargs['cache_key'])
            ttl = kwargs['cache_ttl']
            del kwargs['cache_key']
            del kwargs['cache_ttl']
            now = reactor.seconds()

            @defer.inlineCallbacks
            def get_value():
                result = yield f(*args, **kwargs)
                defer.returnValue(result)
            timestamp, result = CACHE.get(key, (0, None))
            if timestamp + ttl < now:
                CACHE[key] = (now, result)
                result = yield get_value()
                CACHE[key] = (now, result)
        else:
            result = yield f(*args, **kwargs)
        defer.returnValue(result)
    return wrapper
