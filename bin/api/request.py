import anyjson
import moira
from time import time
from twisted.python import log
from twisted.internet import defer
from twisted.web import http, server
from graphite.evaluator import evaluateTarget
from graphite.datalib import createRequestContext


def bad_request(request, message):
    request.setResponseCode(http.BAD_REQUEST)
    request.write(message)
    request.finish()
    return message


def check_json(f):
    @defer.inlineCallbacks
    def decorator(*args, **kwargs):
        request = args[1]
        try:
            request.body = request.content.getvalue()
            request.body_json = anyjson.deserialize(request.body)
        except:
            log.err()
            defer.returnValue(bad_request(request, "Content is not json"))
        yield f(*args, **kwargs)
    return decorator


def check_trigger(f):
    @defer.inlineCallbacks
    def decorator(*args, **kwargs):
        request = args[1]
        json = request.body_json
        request.graphite_patterns = []
        for field in ["targets", "warn_value", "error_value"]:
            if json.get(field) is None:
                defer.returnValue(
                    bad_request(
                        request,
                        "%s is required" %
                        field))
        try:
            request.body_json = moira.reformat_trigger(json, json.get("id"), json.get("tags", []))
        except:
            log.err()
            defer.returnValue(bad_request(request, "Invalid trigger format"))
        try:
            now = int(time())
            requestContext = createRequestContext(str(now - 10), str(now))
            resolved = set()
            for target in json["targets"]:
                yield evaluateTarget(requestContext, target)
                for pattern, resolve in requestContext['graphite_patterns'].iteritems():
                    for r in resolve:
                        if r != pattern:
                            resolved.add(r)
            request.body_json["patterns"] = [pattern for pattern in requestContext['graphite_patterns']
                                             if pattern not in resolved]
        except:
            log.err()
            defer.returnValue(bad_request(request, "Invalid graphite target"))
        yield f(*args, **kwargs)
    return decorator


def delayed(f):
    def decorator(resource, request):
        @defer.inlineCallbacks
        def wrapper():
            try:
                yield f(resource, request)
            except:
                log.err()
                request.setResponseCode(http.INTERNAL_SERVER_ERROR)
                request.finish()
        wrapper()
        return server.NOT_DONE_YET
    return decorator
