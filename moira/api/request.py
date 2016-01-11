import anyjson
from time import time
from twisted.python import log
from twisted.internet import defer
from twisted.web import http, server
from moira.graphite.evaluator import evaluateTarget
from moira.graphite.datalib import createRequestContext
from moira.trigger import trigger_reformat
from moira.checker.expression import getExpression


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
            request.body_json = trigger_reformat(json, json.get("id"), json.get("tags", []))
        except:
            log.err()
            defer.returnValue(bad_request(request, "Invalid trigger format"))
        expression_values = {'warn_value': json.get('warn_value'),
                             'error_value': json.get('error_value')}
        try:
            now = int(time())
            requestContext = createRequestContext(str(now - 10), str(now))
            resolved = set()
            target_num = 1
            for target in json["targets"]:
                target_time_series = yield evaluateTarget(requestContext, target)
                expression_values["t%s" % target_num] = 42 if len(target_time_series) == 0 else target_time_series[-1]
                target_num += 1
                for pattern, resolve in requestContext['graphite_patterns'].iteritems():
                    for r in resolve:
                        if r != pattern:
                            resolved.add(r)
            request.body_json["patterns"] = [pattern for pattern in requestContext['graphite_patterns']
                                             if pattern not in resolved]
        except:
            log.err()
            defer.returnValue(bad_request(request, "Invalid graphite target"))
        try:
            getExpression(json.get("expression"), **expression_values)
        except:
            log.err()
            defer.returnValue(bad_request(request, "Invalid expression"))
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
