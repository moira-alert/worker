import uuid
from urllib import unquote

from twisted.internet import defer
from twisted.web import http

from moira.api.request import delayed, check_json
from moira.api.resources.metric import Metrics
from moira.api.resources.redis import RedisResouce


class State(RedisResouce):

    def __init__(self, db, trigger_id):
        self.trigger_id = trigger_id
        RedisResouce.__init__(self, db)

    @delayed
    @defer.inlineCallbacks
    def render_GET(self, request):
        check = yield self.db.getTriggerLastCheck(self.trigger_id)
        result = {} if check is None else check
        result["trigger_id"] = self.trigger_id
        self.write_json(request, result)


class Throttling(RedisResouce):

    def __init__(self, db, trigger_id):
        self.trigger_id = trigger_id
        RedisResouce.__init__(self, db)

    @delayed
    @defer.inlineCallbacks
    def render_GET(self, request):
        result = yield self.db.getTriggerThrottling(self.trigger_id)
        self.write_json(request, {"throttling": result})

    @delayed
    @defer.inlineCallbacks
    def render_DELETE(self, request):
        yield self.db.deleteTriggerThrottling(self.trigger_id)
        request.finish()


class Maintenance(RedisResouce):

    def __init__(self, db, trigger_id):
        self.trigger_id = trigger_id
        RedisResouce.__init__(self, db)

    @delayed
    @check_json
    @defer.inlineCallbacks
    def render_PUT(self, request):
        yield self.db.setTriggerMetricsMaintenance(self.trigger_id, request.body_json)
        request.finish()


class Trigger(RedisResouce):

    def __init__(self, db, trigger_id):
        self.trigger_id = trigger_id
        RedisResouce.__init__(self, db)
        self.putChild("state", State(db, trigger_id))
        self.putChild("throttling", Throttling(db, trigger_id))
        self.putChild("metrics", Metrics(db, trigger_id))
        self.putChild("maintenance", Maintenance(db, trigger_id))

    @delayed
    @defer.inlineCallbacks
    def render_PUT(self, request):
        yield self.save_trigger(request, self.trigger_id, "trigger updated")

    @delayed
    @defer.inlineCallbacks
    def render_GET(self, request):
        json, trigger = yield self.db.getTrigger(self.trigger_id)
        if json is None:
            request.setResponseCode(http.NOT_FOUND)
            request.finish()
        else:
            throttling = yield self.db.getTriggerThrottling(self.trigger_id)
            trigger["throttling"] = throttling
            self.write_json(request, trigger)

    @delayed
    @defer.inlineCallbacks
    def render_DELETE(self, request):
        _, existing = yield self.db.getTrigger(self.trigger_id)
        yield self.db.removeTrigger(self.trigger_id, request=request, existing=existing)
        request.finish()


class Triggers(RedisResouce):

    def __init__(self, db):
        RedisResouce.__init__(self, db)
        self.putChild("page", Page(db))

    def getChild(self, path, request):
        if not path:
            return self
        return Trigger(self.db, path)

    @delayed
    @defer.inlineCallbacks
    def render_GET(self, request):
        result = yield self.db.getTriggersChecks()
        self.write_json(request, {"list": result})

    @delayed
    @defer.inlineCallbacks
    def render_PUT(self, request):
        trigger_id = str(uuid.uuid4())
        yield self.save_trigger(request, trigger_id, "trigger created")

class Page(RedisResouce):

    def __init__(self, db):
        RedisResouce.__init__(self, db)

    @delayed
    @defer.inlineCallbacks
    def render_GET(self, request):
        filter_ok = request.getCookie('moira_filter_ok')
        filter_tags = request.getCookie('moira_filter_tags')
        page = request.args.get("p")
        size = request.args.get("size")
        page = 0 if page is None else int(page[0])
        size = 10 if size is None else int(size[0])
        filter_ok = False if filter_ok is None else filter_ok == 'true'
        filter_tags = [] if not filter_tags else unquote(filter_tags).split(',')
        if not filter_ok and len(filter_tags) == 0:
            triggers, total = yield self.db.getTriggersChecksPage(page * size, size - 1)
        else:
            triggers, total = yield self.db.getFilteredTriggersChecksPage(page, size, filter_ok, filter_tags)
        self.write_json(request, {"list": triggers, "page": page, "size": size, "total": total})
