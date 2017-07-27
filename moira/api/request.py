from time import time

import anyjson
from moira.graphite.datalib import createRequestContext
from moira.graphite.evaluator import evaluateTarget
from twisted.internet import defer
from twisted.web import http, server

from moira.checker.expression import getExpression
from moira.checker import state
from moira.trigger import trigger_reformat
from moira.logs import log


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
        except Exception as e:
            log.error("Invalid trigger json [{json}]: {e}", json=request.body, e=e)
            defer.returnValue(bad_request(request, "Content is not json"))
        yield f(*args, **kwargs)
    return decorator


def is_simple_target(requestContext):
    if len(requestContext['graphite_patterns']) > 1:
        return False

    complexPatternFound = False
    for pattern in requestContext['graphite_patterns'].iterkeys():
        if '*' in pattern or '{' in pattern:
            complexPatternFound = True
            break

    return not complexPatternFound


@defer.inlineCallbacks
def resolve_patterns(request, expression_values):
    now = int(time())
    context = createRequestContext(str(now - 600), str(now), allowRealTimeAlerting=True)
    resolved = set()
    target_num = 1
    context['time_series_names'] = set()
    is_simple_trigger = True
    if len(request.body_json["targets"]) > 1:
        is_simple_trigger = False
    for target in request.body_json["targets"]:
        time_series = yield evaluateTarget(context, target)
        if is_simple_trigger and not is_simple_target(context):
            is_simple_trigger = False
        target_name = "t%s" % target_num
        for ts in time_series:
            context['time_series_names'].add(ts.name)
        expression_values[target_name] = 42
        target_num += 1
        for pattern, resolve in context['graphite_patterns'].iteritems():
            for r in resolve:
                if r != pattern:
                    resolved.add(r)
    request.body_json["patterns"] = [pattern for pattern in context['graphite_patterns']
                                     if pattern not in resolved]
    request.body_json["is_simple_trigger"] = is_simple_trigger
    request.context = context


def check_trigger(f):
    @defer.inlineCallbacks
    def decorator(*args, **kwargs):
        request = args[1]
        json = request.body_json
        request.graphite_patterns = []
        for field, alt in [("targets", None), ("warn_value", "expression"), ("error_value", "expression")]:
            if json.get(field) is None and json.get(alt) is None:
                defer.returnValue(bad_request(request, "%s is required" % field))
        if type(json["targets"]) is not list:
            defer.returnValue(bad_request(request, "Invalid trigger targets"))
        try:
            request.body_json = trigger_reformat(json, json.get("id"), json.get("tags", []))
        except Exception as e:
            log.error("Invalid trigger format [{json}]: {e}", json=json, e=e)
            defer.returnValue(bad_request(request, "Invalid trigger format"))
        expression_values = {'warn_value': json.get('warn_value'),
                             'error_value': json.get('error_value'),
                             'PREV_STATE': state.NODATA}
        try:
            yield resolve_patterns(request, expression_values)
        except Exception as e:
            log.error("Invalid graphite targets [{targets}]: {e}", targets=request.body_json["targets"], e=e)
            defer.returnValue(bad_request(request, "Invalid graphite targets"))
        try:
            getExpression(json.get("expression"), **expression_values)
        except Exception as e:
            log.error("Invalid expression [{expression}]: {e}", expression=json.get("expression"), e=e)
            defer.returnValue(bad_request(request, "Invalid expression"))
        yield f(*args, **kwargs)
    return decorator


def delayed(f):
    def decorator(resource, request):
        @defer.inlineCallbacks
        def wrapper():
            try:
                yield f(resource, request)
            except Exception as e:
                log.error("Error in delayed decorator wrapped function: {e}", e=e)
                request.setResponseCode(http.INTERNAL_SERVER_ERROR)
                request.finish()
        wrapper()
        return server.NOT_DONE_YET
    return decorator
