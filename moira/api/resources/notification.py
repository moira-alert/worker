from twisted.internet import defer

from moira.api.request import delayed
from moira.api.resources.redis import RedisResource


class Notifications(RedisResource):

    def __init__(self, db):
        RedisResource.__init__(self, db)

    @delayed
    @defer.inlineCallbacks
    def render_GET(self, request):
        notifications, total = yield self.db.getNotifications(request.args.get('start')[0],
                                                              request.args.get('end')[0])
        self.write_json(request, {"list": list(notifications), "total": total})

    @delayed
    @defer.inlineCallbacks
    def render_DELETE(self, request):
        result = yield self.db.removeNotification(request.args.get('id')[0])
        self.write_json(request, {"result": result})

    def getChild(self, path, request):
        if not path:
            return self
