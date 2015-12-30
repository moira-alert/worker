from moira.api.request import delayed
from twisted.internet import defer
from moira.api.resources.redis import RedisResouce
from moira.graphite.evaluator import evaluateTarget
from moira.graphite.datalib import createRequestContext
from moira.api.request import bad_request


class Metrics(RedisResouce):

    def __init__(self, db, trigger_id):
        self.trigger_id = trigger_id
        RedisResouce.__init__(self, db)

    @delayed
    @defer.inlineCallbacks
    def render_GET(self, request):
        json, trigger = yield self.db.getTrigger(self.trigger_id)
        if json is None:
            defer.returnValue(bad_request(request, "Trigger not found"))
            raise StopIteration

        requestContext = createRequestContext(request.args.get('from')[0],
                                              request.args.get('to')[0])
        result = {}
        for target in trigger.get("targets", [trigger.get("target")]):
            time_series = yield evaluateTarget(requestContext, target)
            for time_serie in time_series:
                values = [(time_serie.start + time_serie.step * i, time_serie[i]) for i in range(0, len(time_serie))]
                result[time_serie.name] = [{"ts": ts, "value": value} for ts, value in values if value is not None]
        self.write_json(request, result)

    @delayed
    @defer.inlineCallbacks
    def render_DELETE(self, request):
        metric = request.args.get('name')[0]
        last_check = yield self.db.getTriggerLastCheck(self.trigger_id)

        if last_check is None:
            defer.returnValue(bad_request(request, "Trigger check not found"))
            raise StopIteration

        json, trigger = yield self.db.getTrigger(self.trigger_id)

        if json is None:
            defer.returnValue(bad_request(request, "Trigger not found"))
            raise StopIteration

        metrics = last_check.get('metrics')
        if metrics and metric in metrics:
            del last_check['metrics'][metric]
            for pattern in trigger.get("patterns"):
                yield self.db.delPatternMetrics(pattern, request=request)
        yield self.db.setTriggerLastCheck(self.trigger_id, last_check)
        request.finish()
