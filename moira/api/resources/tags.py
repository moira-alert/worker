from twisted.web import http
from twisted.internet import defer
from moira.api.resources.redis import RedisResouce
from moira.api.request import delayed, check_json


class Stats(RedisResouce):

    def __init__(self, db):
        RedisResouce.__init__(self, db)

    @delayed
    @defer.inlineCallbacks
    def render_GET(self, request):
        tags = yield self.db.getTags()
        result = []
        for tag in tags:
            triggers = yield self.db.getTagTriggers(tag)
            subs = yield self.db.getTagSubscriptions(tag)
            data = yield self.db.getTag(tag)
            tag_data = {
                "name": tag,
                "triggers": triggers,
                "subscriptions": subs,
                "data": data}
            result.append(tag_data)
        self.write_json(request, {"list": result})


class Data(RedisResouce):

    def __init__(self, db, tag):
        self.tag = tag
        RedisResouce.__init__(self, db)

    @delayed
    @check_json
    @defer.inlineCallbacks
    def render_PUT(self, request):
        existing = yield self.db.getTag(self.tag)
        yield self.db.setTag(self.tag, request.body_json, request=request, existing=existing)
        request.finish()


class Tag(RedisResouce):

    def __init__(self, db, tag):
        self.tag = tag
        RedisResouce.__init__(self, db)
        self.putChild("data", Data(db, tag))

    @delayed
    @defer.inlineCallbacks
    def render_DELETE(self, request):
        triggers = yield self.db.getTagTriggers(self.tag)
        if len(triggers) > 0:
            request.setResponseCode(http.BAD_REQUEST)
            request.write(
                "This tag is assigned to %s triggers. Remove tag from triggers first" %
                len(triggers))
            request.finish()
        else:
            existing = yield self.db.getTag(self.tag)
            yield self.db.removeTag(self.tag, request=request, existing=existing)
            self.write_json(request, {"message": "tag deleted"})


class Tags(RedisResouce):

    def __init__(self, db):
        RedisResouce.__init__(self, db)
        self.putChild("stats", Stats(db))

    def getChild(self, path, request):
        if not path:
            return self
        return Tag(self.db, path)

    @delayed
    @defer.inlineCallbacks
    def render_GET(self, request):
        tags = yield self.db.getTags()
        self.write_json(request, {"tags": tags, "list": [k for k in tags]})
