from moira.api.request import delayed
from twisted.internet import defer
from moira.api.resources.redis import RedisResouce


class Events(RedisResouce):

    def __init__(self, db, trigger_id=None):
        self.trigger_id = trigger_id
        RedisResouce.__init__(self, db)

    @delayed
    @defer.inlineCallbacks
    def render_GET(self, request):
        events = yield self.db.getEvents(trigger_id=self.trigger_id, start=-100, end=-1)
        self.write_json(request, {"list": events})

    def getChild(self, path, request):
        if not path:
            return self
        return Events(self.db, path)
