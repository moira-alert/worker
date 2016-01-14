import anyjson
from twisted.web.resource import Resource
from twisted.internet import defer
from moira.api.request import check_trigger, check_json


class RedisResouce(Resource):

    def __init__(self, db):
        Resource.__init__(self)
        self.db = db

    def write_json(self, request, result):
        request.setHeader("Content-Type", "application/json")
        request.write(anyjson.serialize(result))
        request.finish()

    def write_dumped_json(self, request, result):
        request.setHeader("Content-Type", "application/json")
        request.write(str(result))
        request.finish()

    @check_json
    @check_trigger
    @defer.inlineCallbacks
    def save_trigger(self, request, trigger_id, message):
        _, existing = yield self.db.getTrigger(trigger_id, tags=True)
        yield self.db.saveTrigger(trigger_id, request.body_json,
                                  request=request, existing=existing)
        self.write_json(request, {
            "id": trigger_id,
            "message": message
        })
