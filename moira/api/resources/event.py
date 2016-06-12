from twisted.internet import defer

from moira.api.request import delayed
from moira.api.resources.redis import RedisResouce


class Events(RedisResouce):

    def __init__(self, db, trigger_id=None):
        self.trigger_id = trigger_id
        RedisResouce.__init__(self, db)

    @delayed
    @defer.inlineCallbacks
    def render_GET(self, request):
        page = request.args.get("p")
        size = request.args.get("size")
        page = 0 if page is None else int(page[0])
        size = 100 if size is None else int(size[0])
        events, total = yield self.db.getEvents(trigger_id=self.trigger_id, start=page * size, size=size - 1)
        self.write_json(request, {"list": events, "page": page, "size": size, "total": total})

    def getChild(self, path, request):
        if not path:
            return self
        return Events(self.db, path)
