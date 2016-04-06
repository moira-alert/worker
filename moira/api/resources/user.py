from twisted.internet import defer

from moira.api.request import delayed
from moira.api.resources.redis import RedisResouce


class Login(RedisResouce):

    def __init__(self, db):
        RedisResouce.__init__(self, db)
        self.putChild("settings", Settings(db))

    @delayed
    def render_GET(self, request):
        login = request.login
        self.write_json(request, {'login': login})


class Settings(RedisResouce):

    def __init__(self, db):
        RedisResouce.__init__(self, db)

    @delayed
    @defer.inlineCallbacks
    def render_GET(self, request):
        login = request.login
        settings = {"login": login,
                    "subscriptions": [],
                    "contacts": []}
        subs = yield self.db.getUserSubscriptions(login)
        contacts = yield self.db.getUserContacts(login)
        yield self.db.join(contacts, self.db.getContact, settings["contacts"])
        yield self.db.join(subs, self.db.getSubscription, settings["subscriptions"])
        self.write_json(request, settings)
