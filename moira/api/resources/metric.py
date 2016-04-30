from moira.graphite.datalib import createRequestContext
from moira.graphite.evaluator import evaluateTarget
from twisted.internet import defer

from moira.api.request import bad_request
from moira.api.request import delayed
from moira.api.resources.redis import RedisResouce


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

        context = createRequestContext(request.args.get('from')[0],
                                       request.args.get('to')[0])
        result = {}
        for target in trigger.get("targets", [trigger.get("target")]):
            time_series = yield evaluateTarget(context, target)
            for time_serie in time_series:
                values = [(time_serie.start + time_serie.step * i, time_serie[i]) for i in range(0, len(time_serie))]
                result[time_serie.name] = [{"ts": ts, "value": value} for ts, value in values if value is not None]
        self.write_json(request, result)

    @delayed
    @defer.inlineCallbacks
    def render_DELETE(self, request):
        metric = request.args.get('name')[0]

        json, trigger = yield self.db.getTrigger(self.trigger_id)

        if json is None:
            defer.returnValue(bad_request(request, "Trigger not found"))
            raise StopIteration

        yield self.db.accuireTriggerCheckLock(self.trigger_id, 10)

        last_check = yield self.db.getTriggerLastCheck(self.trigger_id)

        if last_check is None:
            defer.returnValue(bad_request(request, "Trigger check not found"))
            raise StopIteration

        metrics = last_check.get('metrics', {})
        if metric in metrics:
            del last_check['metrics'][metric]

        for pattern in trigger.get("patterns"):
            yield self.db.delPatternMetrics(pattern)

        yield self.db.setTriggerLastCheck(self.trigger_id, last_check)
        yield self.db.delTriggerCheckLock(self.trigger_id)

        request.finish()
