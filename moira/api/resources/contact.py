from twisted.internet import defer

from moira.api.request import delayed, check_json
from moira.api.resources.redis import RedisResource


class Contact(RedisResource):

    def __init__(self, db, contact_id):
        self.contact_id = contact_id
        RedisResource.__init__(self, db)

    @delayed
    @defer.inlineCallbacks
    def render_DELETE(self, request):
        login = request.login
        existing = yield self.db.getContact(self.contact_id)
        yield self.db.deleteUserContact(self.contact_id, login,
                                        request=request, existing=existing)
        request.finish()


class Contacts(RedisResource):
    def getChild(self, path, request):
        if not path:
            return self
        return Contact(self.db, path)

    @delayed
    @defer.inlineCallbacks
    def render_GET(self, request):
        contacts = yield self.db.getAllContacts()
        self.write_json(request, {'list': contacts})

    @delayed
    @check_json
    @defer.inlineCallbacks
    def render_PUT(self, request):
        existing_id = request.body_json.get("id")
        existing = None if existing_id is None else (yield self.db.getContact(existing_id))
        contact = yield self.db.saveUserContact(request.login, request.body_json,
                                                request=request,
                                                existing=existing)
        self.write_json(request, contact)
