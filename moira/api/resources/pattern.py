from moira.api.request import delayed
from twisted.internet import defer
from moira.api.resources.redis import RedisResouce


class Pattern(RedisResouce):

    def __init__(self, db, pattern):
        self.pattern = pattern
        RedisResouce.__init__(self, db)

    @delayed
    @defer.inlineCallbacks
    def render_DELETE(self, request):
        yield self.db.removePattern(self.pattern, request=request)
        request.finish()


class Patterns(RedisResouce):

    def __init__(self, db):
        RedisResouce.__init__(self, db)

    def getChild(self, path, request):
        if not path:
            return self
        return Pattern(self.db, path)

    @delayed
    @defer.inlineCallbacks
    def render_GET(self, request):
        result = []
        patterns = yield self.db.getPatterns()
        for pattern in patterns:
            triggers = yield self.db.getPatternTriggers(pattern)
            triggers_list = []
            for trigger_id in triggers:
                _, trigger = yield self.db.getTrigger(trigger_id, tags=True)
                triggers_list.append(trigger)
            metrics = yield self.db.getPatternMetrics(pattern)
            item = {
                "pattern": pattern,
                "triggers": triggers_list,
                "metrics": metrics}
            result.append(item)
        self.write_json(request, {"list": result})
