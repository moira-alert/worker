from twisted.internet import reactor
from twisted.web import server, http
from twisted.web.resource import Resource

from moira import config
from moira.api.resources.contact import Contacts
from moira.api.resources.event import Events
from moira.api.resources.notification import Notifications
from moira.api.resources.pattern import Patterns
from moira.api.resources.subscription import Subscriptions
from moira.api.resources.tags import Tags
from moira.api.resources.trigger import Triggers
from moira.api.resources.user import Login


class MoiraRequest(server.Request):

    def __init__(self, channel, queued):
        self.creation = reactor.seconds()
        self.body_json = None
        server.Request.__init__(self, channel, queued)

    @property
    def login(self):
        return self.getHeader('x-webauth-user') or ''


class Site(server.Site):

    requestFactory = MoiraRequest
    displayTracebacks = False

    def __init__(self, db):
        self.prefix = ""
        root = Resource()
        prefix = root
        for path in config.PREFIX.split('/'):
            if len(path):
                r = Resource()
                prefix.putChild(path, r)
                prefix = r
                self.prefix += "/%s" % path
        prefix.putChild("trigger", Triggers(db))
        prefix.putChild("tag", Tags(db))
        prefix.putChild("pattern", Patterns(db))
        prefix.putChild("event", Events(db))
        prefix.putChild("contact", Contacts(db))
        prefix.putChild("subscription", Subscriptions(db))
        prefix.putChild("user", Login(db))
        prefix.putChild("notification", Notifications(db))
        server.Site.__init__(self, root)

    def _escape(self, s):
        if hasattr(http, '_escape'):
            return http._escape(s)
        return server.Site._escape(self, s)

    def log(self, request):
        if hasattr(self, "logFile"):
            elapsed = reactor.seconds() - request.creation
            line = '- %.3f "%s" %d %s\n' % (
                elapsed,
                '%s %s %s' % (self._escape(request.method),
                              self._escape(request.uri),
                              self._escape(request.clientproto)),
                request.code,
                request.requestHeaders.getRawHeaders('Content-Length', ["-"])[0])
            self.logFile.write(line)
