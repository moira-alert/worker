from moira.graphite.attime import parseATTime
from moira.graphite.util import epoch
from twisted.internet import defer

from moira.api.request import delayed, check_json
from moira.api.resources.redis import RedisResouce


class Test(RedisResouce):

    def __init__(self, db, sub_id):
        self.sub_id = sub_id
        RedisResouce.__init__(self, db)

    @delayed
    @defer.inlineCallbacks
    def render_PUT(self, request):
        yield self.db.pushEvent({
            "sub_id": self.sub_id,
            "metric": "Test.metric.value",
            "value": 1,
            "old_state": "TEST",
            "state": "TEST",
            "timestamp": int(epoch(parseATTime("now")))
        }, ui=False, request=request)
        request.finish()


class Subscription(RedisResouce):

    def __init__(self, db, sub_id):
        self.sub_id = sub_id
        RedisResouce.__init__(self, db)
        self.putChild("test", Test(db, sub_id))

    @delayed
    @defer.inlineCallbacks
    def render_DELETE(self, request):
        login = request.getLogin()
        existing = yield self.db.getSubscription(self.sub_id)
        yield self.db.removeUserSubscription(login, self.sub_id, request=request, existing=existing)
        request.finish()


class Subscriptions(RedisResouce):

    def __init__(self, db):
        RedisResouce.__init__(self, db)

    def getChild(self, path, request):
        if not path:
            return self
        return Subscription(self.db, path)

    @delayed
    @defer.inlineCallbacks
    def render_GET(self, request):
        login = request.getLogin()
        subs = yield self.db.getUserSubscriptions(login)
        result = []
        yield self.db.join(subs, self.db.getSubscription, result)
        self.write_json(request, {'list': result})

    @delayed
    @check_json
    @defer.inlineCallbacks
    def render_PUT(self, request):
        login = request.getLogin()
        get_existing = self.db.getSubscription(request.body_json.get('id'))
        sub = yield self.db.saveUserSubscription(login, request.body_json, request=request,
                                                 get_existing=get_existing)
        self.write_json(request, sub)
