import sys
import time
from functools import wraps
from uuid import uuid4

import anyjson
import txredisapi as redis
from twisted.application import service
from twisted.internet import defer, task, reactor

from moira import config
from moira.cache import cache
from moira import logs
from moira.trigger import trigger_reformat

_doc_string = """
Redis database objects:
    - KEY {0}
    - SET {1}
    - SET {2}
    - KEY {3}
    - LIST {4}
    - LIST {5}
    - SORTED SET {6}
    - KEY {7}
    - SET {8}
    - SET {9}
    - SET {10}
    - SET {11}
    - SET {12}
    - SET {13}
    - SET {14}
    - SET {15}
    - KEY {16}
    - KEY {17}
    - SET {18}
    - SORTED SET {19}
    - SET {20}
    - KEY {21}
    - KEY {22}
    - SORTED SET {23}
    - SET {24}
    - KEY {25}
"""

__docformat__ = 'reStructuredText'


LAST_CHECK_PREFIX = "moira-metric-last-check:{0}"
PATTERN_METRICS_PREFIX = "moira-pattern-metrics:{0}"
PATTERN_TRIGGERS_PREFIX = "moira-pattern-triggers:{0}"
TRIGGER_PREFIX = "moira-trigger:{0}"
EVENTS = "moira-trigger-events"
EVENTS_UI = "moira-trigger-events-ui"
TRIGGERS_CHECKS = "moira-triggers-checks"
METRIC_OLD_PREFIX = "moira-metric:{0}"
METRIC_PREFIX = "moira-metric-data:{0}"
METRIC_RETENTION_PREFIX = "moira-metric-retention:{0}"
TRIGGERS = "moira-triggers-list"
PATTERNS = "moira-pattern-list"
TRIGGER_TAGS_PREFIX = "moira-trigger-tags:{0}"
TAGS = "moira-tags"
TAG_TRIGGERS_PREFIX = "moira-tag-triggers:{0}"
TRIGGERS_TO_CHECK = "moira-triggers-tocheck"
USER_SUBSCRIPTIONS_PREFIX = "moira-user-subscriptions:{0}"
TAG_SUBSCRIPTIONS_PREFIX = "moira-tag-subscriptions:{0}"
SUBSCRIPTION_PREFIX = "moira-subscription:{0}"
CONTACT_PREFIX = "moira-contact:{0}"
USER_CONTACTS_PREFIX = "moira-user-contacts:{0}"
TRIGGER_EVENTS = "moira-trigger-events:{0}"
TRIGGER_THROTTLING_BEGINNING_PREFIX = "moira-notifier-throttling-beginning:{0}"
TRIGGER_NEXT_PREFIX = "moira-notifier-next:{0}"
NOTIFIER_NOTIFICATIONS = "moira-notifier-notifications"
TAG_PREFIX = "moira-tag:{0}"
TRIGGER_CHECK_LOCK_PREFIX = "moira-metric-check-lock:{0}"
TRIGGER_IN_BAD_STATE = "moira-bad-state-triggers"
CHECKS_COUNTER = "moira-selfstate:checks-counter"

TRIGGER_EVENTS_TTL = 3600 * 24 * 30

current_module = sys.modules[__name__]
current_module.__doc__ = _doc_string.format(
    LAST_CHECK_PREFIX.format("<trigger_id>"),
    PATTERN_METRICS_PREFIX.format("<pattern>"),
    PATTERN_TRIGGERS_PREFIX.format("<pattern>"),
    TRIGGER_PREFIX.format("<trigger_id>"),
    EVENTS,
    EVENTS_UI,
    METRIC_PREFIX.format("<metric>"),
    METRIC_RETENTION_PREFIX.format("<metric>"),
    TRIGGERS,
    PATTERNS,
    TRIGGER_TAGS_PREFIX.format("<trigger_id>"),
    TAGS,
    TAG_TRIGGERS_PREFIX.format("<tag>"),
    TRIGGERS_TO_CHECK,
    USER_SUBSCRIPTIONS_PREFIX.format("<login>"),
    TAG_SUBSCRIPTIONS_PREFIX.format("<tag>"),
    SUBSCRIPTION_PREFIX.format("<subscription_id>"),
    CONTACT_PREFIX.format("<contact_id>"),
    USER_CONTACTS_PREFIX.format("<login>"),
    TRIGGER_EVENTS.format("<trigger_id>"),
    TRIGGER_THROTTLING_BEGINNING_PREFIX.format("<trigger_id>"),
    TAG_PREFIX.format("<tag>"),
    TRIGGER_CHECK_LOCK_PREFIX.format("trigger_id"),
    TRIGGERS_CHECKS,
    TRIGGER_IN_BAD_STATE,
    CHECKS_COUNTER
)


def docstring_parameters(*sub):
    def dec(obj):
        obj.__doc__ = obj.__doc__.format(*sub)
        return obj
    return dec


audit_log = None


def audit(f):
    """
    Write json object changes to audit.log
    """
    @wraps(f)
    @defer.inlineCallbacks
    def decorator(*args, **kwargs):
        global audit_log
        if audit_log is None:
            audit_log = logs.audit()
        if 'existing' not in kwargs:
            get_existing = kwargs.get('get_existing')
            kwargs['existing'] = yield get_existing
        existing = kwargs.get('existing', {})
        request = kwargs.get('request')
        if request:
            source = {} if request.body_json is None else request.body_json
            existing = {} if existing is None else existing
            additions = [(k, source[k]) for k in source if k not in existing or source[k] != existing[k]]
            deletions = [(k, existing[k]) for k in existing if k not in source or source[k] != existing[k]]
            audit_log.info("{request.login}\t{request.method}\t{request.uri}", request=request)
            for key, add in additions:
                audit_log.info("\t+ {key}:{add}", key=key, add=add)
            for key, deletion in deletions:
                audit_log.info("\t- {key}:{deletion}", key=key, deletion=deletion)
        for a in ['request', 'get_existing']:
            if a in kwargs:
                del kwargs[a]
        result = yield f(*args, **kwargs)
        defer.returnValue(result)
    return decorator


class Db(service.Service):

    """
    Redis database service class
    """

    def __init__(self):
        self.rc = None

    @defer.inlineCallbacks
    def startService(self):
        """
        startService(self)
        Creates redis connection pool
        """
        if self.rc is None:
            self.rc = yield redis.ConnectionPool(config.REDIS_HOST, config.REDIS_PORT, dbid=config.DBID)
        defer.returnValue(self)

    @audit
    @defer.inlineCallbacks
    @docstring_parameters(SUBSCRIPTION_PREFIX.format("<sub_id>"), USER_SUBSCRIPTIONS_PREFIX.format("<login>"))
    def saveUserSubscription(self, login, sub, existing=None):
        """
        saveUserSubscription(self, login, sub)

        Creates redis transaction for:
            - save *sub* json to key {0}
            - add *sub_id* to set {1}

        :param login: user login
        :type login: string
        :param sub: subscription data
        :type sub: json dict
        :rtype: json dict
        """
        sub_id = sub.get("id")
        existing = existing
        if existing is None:
            sub_id = str(uuid4())
        t = yield self.rc.multi()
        sub["user"] = login
        sub["id"] = sub_id
        sub_tags = sub.get("tags", [])
        if existing is not None:
            for tag in existing.get("tags", []):
                yield t.srem(TAG_SUBSCRIPTIONS_PREFIX.format(tag), sub_id)
        for tag in sub_tags:
            yield t.sadd(TAG_SUBSCRIPTIONS_PREFIX.format(tag), sub_id)
        yield t.sadd(USER_SUBSCRIPTIONS_PREFIX.format(login), sub_id)
        yield t.set(SUBSCRIPTION_PREFIX.format(sub_id), anyjson.serialize(sub))
        yield t.commit()
        defer.returnValue(sub)

    @audit
    @defer.inlineCallbacks
    @docstring_parameters(CONTACT_PREFIX.format("<contact_id>"), USER_CONTACTS_PREFIX.format("login"))
    def saveUserContact(self, login, contact, existing=None):
        """
        saveUserContact(self, login, contact)

        Creates redis transaction for:
            - save *contact* json to key {0}
            - add *contact_id* to set {1}

        :param login: user login
        :type login: string
        :param contact: contact data
        :type contact: json dict
        :rtype: json dict
        """
        contact["user"] = login
        contact_id = contact.get("id", str(uuid4()))
        contact["id"] = contact_id
        t = yield self.rc.multi()
        yield t.set(CONTACT_PREFIX.format(contact_id), anyjson.serialize(contact))
        yield t.sadd(USER_CONTACTS_PREFIX.format(login), contact_id)
        yield t.commit()
        defer.returnValue(contact)

    @audit
    @defer.inlineCallbacks
    @docstring_parameters(CONTACT_PREFIX.format("<contact_id>"), USER_CONTACTS_PREFIX.format("login"))
    def deleteUserContact(self, contact_id, login, existing=None):
        """
        deleteUserContact(self, contact_id, login)

        Creates redis transaction for:
            - remove key {0}
            - remove *contact_id* from set {1}

        :param contact_id: contact id
        :type contact_id: string
        :param login: user login
        :type login: string
        """
        changed_subs = []
        subs = yield self.getUserSubscriptions(login)
        for sub_id in subs:
            sub = yield self.getSubscription(sub_id)
            if sub and contact_id in sub["contacts"]:
                sub["contacts"].remove(contact_id)
                changed_subs.append(sub)
        t = yield self.rc.multi()
        yield t.delete(CONTACT_PREFIX.format(contact_id))
        yield t.srem(USER_CONTACTS_PREFIX.format(login), contact_id)
        for sub in changed_subs:
            yield t.set(SUBSCRIPTION_PREFIX.format(sub["id"]), anyjson.serialize(sub))
        yield t.commit()

    @audit
    @defer.inlineCallbacks
    @docstring_parameters(SUBSCRIPTION_PREFIX.format("subscription_id"), USER_SUBSCRIPTIONS_PREFIX.format("login"))
    def removeUserSubscription(self, login, sub_id, existing=None):
        """
        removeUserSubscription(self, login, sub_id)

        Creates redis transaction for:
            - delete key {0}
            - remove *sub_id* from set {1}

        :param login: user login
        :type login: string
        :param sub_id: subscription id
        :type sub_id: string
        """
        t = yield self.rc.multi()
        yield t.srem(USER_SUBSCRIPTIONS_PREFIX.format(login), sub_id)
        if existing is not None:
            for tag in existing.get("tags", []):
                yield t.srem(TAG_SUBSCRIPTIONS_PREFIX.format(tag), sub_id)
            yield t.delete(SUBSCRIPTION_PREFIX.format(sub_id))
        yield t.commit()

    @defer.inlineCallbacks
    @docstring_parameters(USER_SUBSCRIPTIONS_PREFIX.format("<login>"))
    def getUserSubscriptions(self, login):
        """
        getUserSubscriptions(self, login)

        Returns subscriptions ids by given login from set {0}

        :param login: user login
        :type login: string
        :rtype: set of strings
        """
        result = yield self.rc.smembers(USER_SUBSCRIPTIONS_PREFIX.format(login))
        defer.returnValue(result)

    @defer.inlineCallbacks
    @docstring_parameters(USER_CONTACTS_PREFIX.format("<login>"))
    def getUserContacts(self, login):
        """
        getUserContacts(self, login)

        Returns contacts ids by given login from set {0}

        :param login: user login
        :type login: string
        :rtype: set of strings
        """
        result = yield self.rc.smembers(USER_CONTACTS_PREFIX.format(login))
        defer.returnValue(result)

    @defer.inlineCallbacks
    @docstring_parameters(USER_CONTACTS_PREFIX.format("<login>"))
    def getAllContacts(self):
        """
        getAllContacts(self, login)

        Returns all contacts json

        :rtype: array of strings
        """
        result = []
        keys = yield self.rc.keys(CONTACT_PREFIX.format('*'))
        for key in keys:
            contact_id = key.split(':')[-1]
            contact = yield self.getContact(contact_id)
            if contact:
                result.append(contact)
        defer.returnValue(result)

    @defer.inlineCallbacks
    @docstring_parameters(SUBSCRIPTION_PREFIX.format("subscription_id"))
    def getSubscription(self, sub_id):
        """
        getSubscription(self, sub_id)

        Returns subscription by given id from key {0}

        :param sub_id: subscription id
        :type sub_id: string
        :rtype: json dict
        """
        sub_json = yield self.rc.get(SUBSCRIPTION_PREFIX.format(sub_id))
        sub = None
        if sub_json is not None:
            sub = anyjson.loads(sub_json)
            sub["id"] = sub_id
        defer.returnValue(sub)

    @defer.inlineCallbacks
    @docstring_parameters(CONTACT_PREFIX.format("contact_id"))
    def getContact(self, contact_id):
        """
        getContact(self, contact_id)

        Returns contact by given id from key {0}

        :param contact_id: contact id
        :type contact_id: string
        :rtype: json dict
        """
        contact_json = yield self.rc.get(CONTACT_PREFIX.format(contact_id))
        contact = None
        if contact_json is not None:
            contact = anyjson.loads(contact_json)
            contact["id"] = contact_id
        defer.returnValue(contact)

    @defer.inlineCallbacks
    @docstring_parameters(TAG_SUBSCRIPTIONS_PREFIX.format("<tag>"))
    def getTagSubscriptions(self, tag):
        """
        getTagSubscriptions(self, tag)

        Returns all subscriptions by given tag from set {0}

        :type tag: string
        :rtype: list of strings
        """
        result = []
        subscriptions_ids = yield self.rc.smembers(TAG_SUBSCRIPTIONS_PREFIX.format(tag))
        for sub_id in subscriptions_ids:
            sub_json = yield self.rc.get(SUBSCRIPTION_PREFIX.format(sub_id))
            if sub_json is not None:
                sub = anyjson.loads(sub_json)
                sub["id"] = sub_id
                result.append(sub)
            else:
                yield self.rc.srem(TAG_SUBSCRIPTIONS_PREFIX.format(tag), sub_id)
        defer.returnValue(result)

    @audit
    @defer.inlineCallbacks
    @docstring_parameters(
        TRIGGER_PREFIX.format("<trigger_id>"),
        TRIGGERS,
        PATTERNS,
        PATTERN_TRIGGERS_PREFIX.format("<pattern>"))
    def saveTrigger(self, trigger_id, trigger, existing=None):
        """
        saveTrigger(self, trigger_id, trigger)

        Creates redis transaction for:
            - Saving *trigger_json* to key {0}
            - Add *trigger_id* to set {1}
            - Update patterns set {2}
            - Update trigger patterns set {3}

        :param trigger: trigger json object
        :type trigger: dict
        :param trigger_id: trigger identity
        :type trigger_id: string
        """
        ttl = trigger.get("ttl")
        if ttl is not None:
            trigger["ttl"] = str(ttl)
        tags = trigger.get("tags", [])
        t = yield self.rc.multi()
        patterns = trigger.get("patterns", [])
        cleanup_patterns = []
        if existing is not None:
            for pattern in [
                item for item in existing.get(
                    "patterns",
                    []) if item not in patterns]:
                yield t.srem(PATTERN_TRIGGERS_PREFIX.format(pattern), trigger_id)
                cleanup_patterns.append(pattern)
            for tag in [
                item for item in existing.get(
                    "tags",
                    []) if item not in tags]:
                yield self.removeTriggerTag(trigger_id, tag, t)
        yield t.set(TRIGGER_PREFIX.format(trigger_id), anyjson.serialize(trigger))
        yield t.sadd(TRIGGERS, trigger_id)
        for pattern in patterns:
            yield t.sadd(PATTERNS, pattern)
            yield t.sadd(PATTERN_TRIGGERS_PREFIX.format(pattern), trigger_id)
        for tag in tags:
            yield self.addTriggerTag(trigger_id, tag, t)
        yield t.commit()
        for pattern in cleanup_patterns:
            triggers = yield self.getPatternTriggers(pattern)
            if not triggers:
                yield self.removePatternTriggers(pattern)
                yield self.removePattern(pattern)
                yield self.delPatternMetrics(pattern)

    @defer.inlineCallbacks
    @docstring_parameters(PATTERNS)
    def getPatterns(self):
        """
        getPatterns(self)

        Returns all patterns from SET {0}

        :rtype: set of strings
        """
        patterns = yield self.rc.smembers(PATTERNS)
        defer.returnValue(patterns)

    @cache
    @defer.inlineCallbacks
    @docstring_parameters(TRIGGERS_TO_CHECK)
    def addTriggerCheck(self, trigger_id):
        """
        addTriggerCheck(self, trigger_id)

        Add *trigger_id* to set {0}

        :param trigger_id: trigger identity
        :type trigger_id: string
        """
        yield self.rc.sadd(TRIGGERS_TO_CHECK, trigger_id)

    @defer.inlineCallbacks
    @docstring_parameters(TRIGGERS_TO_CHECK)
    def getTriggerToCheck(self):
        """
        getTriggerToCheck(self)

        Pop trigger id from set {0}

        :rtype: string
        """
        trigger_id = yield self.rc.spop(TRIGGERS_TO_CHECK)
        defer.returnValue(trigger_id)

    @cache
    @defer.inlineCallbacks
    @docstring_parameters(TRIGGER_PREFIX.format("<trigger_id>"))
    def getTrigger(self, trigger_id):
        """
        getTrigger(self, trigger_id)

        - Read trigger by key {0}
        - Unpack trigger json

        :param tags: get with tags
        :type tags: boolean
        :param trigger_id: trigger identity
        :type trigger_id: string
        :rtype: tuple(json, trigger)
        """
        pipeline = yield self.rc.pipeline()
        pipeline.get(TRIGGER_PREFIX.format(trigger_id))
        pipeline.smembers(TRIGGER_TAGS_PREFIX.format(trigger_id))
        trigger = {}
        json, trigger_tags = yield pipeline.execute_pipeline()
        if json is not None:
            trigger = anyjson.deserialize(json)
            trigger = trigger_reformat(trigger, trigger_id, trigger_tags)
        defer.returnValue((json, trigger))

    @defer.inlineCallbacks
    def _getTriggersChecks(self, triggers_ids):
        triggers = []
        pipeline = yield self.rc.pipeline()
        for trigger_id in triggers_ids:
            pipeline.get(TRIGGER_PREFIX.format(trigger_id))
            pipeline.smembers(TRIGGER_TAGS_PREFIX.format(trigger_id))
            pipeline.get(LAST_CHECK_PREFIX.format(trigger_id))
            pipeline.get(TRIGGER_NEXT_PREFIX.format(trigger_id))
        results = yield pipeline.execute_pipeline()
        slices = [[triggers_ids[i / 4]] + results[i:i + 4] for i in range(0, len(results), 4)]
        for trigger_id, trigger_json, trigger_tags, last_check, throttling in slices:
            if trigger_json is None:
                continue
            trigger = anyjson.deserialize(trigger_json)
            trigger = trigger_reformat(trigger, trigger_id, trigger_tags)
            trigger["last_check"] = None if last_check is None else anyjson.deserialize(last_check)
            trigger["throttling"] = long(throttling) if throttling and time.time() < long(throttling) else 0
            triggers.append(trigger)
        defer.returnValue(triggers)

    @defer.inlineCallbacks
    def getTriggersChecks(self):
        """
        getTriggersChecks(self)

        - Returns all triggers with it check

        :rtype: json
        """
        triggers_ids = list((yield self.getTriggers()))
        triggers = yield self._getTriggersChecks(triggers_ids)
        defer.returnValue(triggers)

    @defer.inlineCallbacks
    @docstring_parameters(TRIGGERS_CHECKS)
    def getTriggersChecksPage(self, start, size):
        """
        getTriggersChecksPage(self, start, size)

        - Returns triggers range from sorted set {0}

        :param start: start position in range
        :type start: integer
        :param start: number of triggers
        :type start: integer
        :rtype: json
        """
        pipeline = yield self.rc.pipeline()
        pipeline.zrevrange(TRIGGERS_CHECKS, start=start, end=(start + size))
        pipeline.zcard(TRIGGERS_CHECKS)
        triggers_ids, total = yield pipeline.execute_pipeline()
        triggers = yield self._getTriggersChecks(triggers_ids)
        defer.returnValue((triggers, total))

    @defer.inlineCallbacks
    @docstring_parameters(TRIGGERS_CHECKS)
    def getFilteredTriggersChecksPage(self, page, size, filter_ok, filter_tags):
        """
        getFilteredTriggersChecksPage(self, page, size, filter_ok, filter_tags)

        - Returns filtered triggers page

        :param start: start position in range
        :type start: integer
        :param start: number of triggers
        :type start: integer
        :param filter_ok: use triggers set in bad state
        :type filter_ok: boolean
        :param filter_tags: use tag triggers set
        :type filter_tags: list of strings
        :rtype: json
        """
        filter_sets = map(lambda tag: TAG_TRIGGERS_PREFIX.format(tag), filter_tags)
        if filter_ok:
            filter_sets.append(TRIGGER_IN_BAD_STATE)
        pipeline = yield self.rc.pipeline()
        pipeline.zrevrange(TRIGGERS_CHECKS, start=0, end=-1)
        for s in filter_sets:
            pipeline.smembers(s)
        triggers_lists = yield pipeline.execute_pipeline()
        total = []
        for id in triggers_lists[0]:
            valid = True
            for s in triggers_lists[1:]:
                if id not in s:
                    valid = False
                    break
            if valid:
                total.append(id)

        filtered_ids = total[page * size: (page + 1) * size]
        triggers = yield self._getTriggersChecks(filtered_ids)
        defer.returnValue((triggers, len(total)))

    @defer.inlineCallbacks
    @docstring_parameters(TRIGGER_NEXT_PREFIX.format("<trigger_id>"))
    def getTriggerThrottling(self, trigger_id):
        """
        getTriggerThrottling(self, trigger_id)

        Returns planning trigger notification timestamp in future or 0 from key {0}

        :param trigger_id: trigger identity
        :type trigger_id: string
        :rtype: long
        """
        timestamp = yield self.rc.get(TRIGGER_NEXT_PREFIX.format(trigger_id))
        defer.returnValue(long(timestamp) if timestamp and time.time() < long(timestamp) else 0)

    @defer.inlineCallbacks
    def setTriggerThrottling(self, trigger_id, timestamp):
        yield self.rc.set(TRIGGER_NEXT_PREFIX.format(trigger_id), timestamp)

    @defer.inlineCallbacks
    def addThrottledEvent(self, trigger_id, timestamp, event):
        yield self.rc.zadd(NOTIFIER_NOTIFICATIONS, timestamp, anyjson.dumps({'event': event}))

    @defer.inlineCallbacks
    @docstring_parameters(NOTIFIER_NOTIFICATIONS)
    def deleteTriggerThrottling(self, trigger_id):
        """
        deleteTriggerThrottling(self, trigger_id)

        Read all planning notifications from sorted set {0}
        and rescheduling it delivery to now

        :param trigger_id: trigger identity
        :type trigger_id: string
        """
        now = int(time.time())
        yield self.rc.set(TRIGGER_THROTTLING_BEGINNING_PREFIX.format(trigger_id), now)
        yield self.rc.delete(TRIGGER_NEXT_PREFIX.format(trigger_id))
        notifications = yield self.rc.zrangebyscore(NOTIFIER_NOTIFICATIONS, withscores=True)
        t = yield self.rc.multi()
        for json, _ in notifications:
            notification = anyjson.loads(json)
            if notification.get('event', {}).get('trigger_id') == trigger_id:
                t.zadd(NOTIFIER_NOTIFICATIONS, now, json)
        yield t.commit()

    @defer.inlineCallbacks
    @docstring_parameters(NOTIFIER_NOTIFICATIONS)
    def getNotifications(self, start, end):
        """
        getNotifications(self, start, end)

        Read all planning notifications from sorted set {0}

        :param start: range start
        :type start: integer
        :param end: range end
        :type end: integer
        """

        pipeline = yield self.rc.pipeline()
        pipeline.zrange(NOTIFIER_NOTIFICATIONS, start=start, end=end)
        pipeline.zcard(NOTIFIER_NOTIFICATIONS)
        jsons, total = yield pipeline.execute_pipeline()
        defer.returnValue((jsons, total))

    @defer.inlineCallbacks
    @docstring_parameters(NOTIFIER_NOTIFICATIONS)
    def removeNotification(self, id):
        """
        removeNotification(self, id)

        Remove planning notification by id string from sorted set {0}

        :param id: notification id string
        :type id: string
        """
        notifications, total = yield self.getNotifications(0, -1)
        for json in notifications:
            notification = anyjson.loads(json)
            timestamp = str(notification.get('timestamp'))
            contact_id = notification.get('contact', {}).get('id')
            sub_id = notification.get('event', {}).get('sub_id')
            idstr = ''.join([timestamp, contact_id, sub_id])
            if idstr == id:
                result = yield self.rc.zrem(NOTIFIER_NOTIFICATIONS, json)
                defer.returnValue(result)

    @defer.inlineCallbacks
    @docstring_parameters(TAG_TRIGGERS_PREFIX.format("<tag>"))
    def getTagTriggers(self, tag):
        """
        getTagTriggers(self, tag)

        Returns all trigger is for given tag from set {0}

        :type tag: string
        :rtype: set of strings
        """
        tags = yield self.rc.smembers(TAG_TRIGGERS_PREFIX.format(tag))
        defer.returnValue(tags)

    @defer.inlineCallbacks
    @docstring_parameters(TAGS)
    def getTags(self):
        """
        getTags(self)

        Returns all tags from set {0} with tag data

        :rtype: dict
        """
        result = {}
        tags = yield self.rc.smembers(TAGS)
        for tag in tags:
            data = yield self.getTag(tag)
            result[unicode(tag)] = data
        defer.returnValue(result)

    @defer.inlineCallbacks
    @docstring_parameters(TAG_PREFIX)
    def getTag(self, tag):
        """
        getTag(self, tag)

        Returns tag data from key {0}

        :type tag: string
        :rtype: json dict
        """
        tag = yield self.rc.get(TAG_PREFIX.format(tag))
        defer.returnValue({} if tag is None else anyjson.loads(tag))

    @audit
    @defer.inlineCallbacks
    @docstring_parameters(TAG_PREFIX)
    def setTag(self, tag, data, existing=None):
        """
        setTag(self, tag, data)

        Save tag data to key {0}

        :type tag: string
        :type data: object
        :rtype: json dict
        """
        yield self.rc.set(TAG_PREFIX.format(tag), anyjson.dumps(data))

    @defer.inlineCallbacks
    @docstring_parameters(
        TRIGGER_TAGS_PREFIX.format("<trigger_id>"),
        TAG_TRIGGERS_PREFIX.format("<tag>"),
        TAGS)
    def addTriggerTag(self, trigger_id, tag, t=None):
        """
        addTriggerTag(self, trigger_id, tag)

        Creates redis transaction for:
            - Add *tag* to set {0}
            - Add *trigger_id* to set {1}
            - Add *tag* to set {2}

        :type tag: string
        :param trigger_id: trigger identity
        :type trigger_id: string
        """
        commit = t is None
        if t is None:
            t = yield self.rc.multi()
        yield t.sadd(TRIGGER_TAGS_PREFIX.format(trigger_id), tag)
        yield t.sadd(TAG_TRIGGERS_PREFIX.format(tag), trigger_id)
        yield t.sadd(TAGS, tag)
        if commit:
            yield t.commit()

    @defer.inlineCallbacks
    @docstring_parameters(
        TRIGGER_TAGS_PREFIX.format("<trigger_id>"),
        TAG_TRIGGERS_PREFIX.format("<tag>"))
    def removeTriggerTag(self, trigger_id, tag, t=None):
        """
        removeTriggerTag(self, trigger_id, tag)

        Creates redis transaction for:
            - Remove *tag* from set {0}
            - Remove *trigger_id* from set {1}

        :type tag: string
        :param trigger_id: trigger identity
        :type trigger_id: string
        """
        commit = t is None
        if t is None:
            t = yield self.rc.multi()
        yield t.srem(TRIGGER_TAGS_PREFIX.format(trigger_id), tag)
        yield t.srem(TAG_TRIGGERS_PREFIX.format(tag), trigger_id)
        if commit:
            yield t.commit()

    @audit
    @defer.inlineCallbacks
    @docstring_parameters(
        TAGS,
        TAG_SUBSCRIPTIONS_PREFIX.format("<tag>"),
        TAG_TRIGGERS_PREFIX.format("<tag>"),
        TAG_PREFIX.format("<tag>"))
    def removeTag(self, tag, existing=None):
        """
        removeTag(self, tag)

        Creates redis transaction for:
            - Remove *tag* from set {0}
            - Delete key {1}
            - Delete key {2}
            - Delete key {3}

        :type tag: string
        """
        t = yield self.rc.multi()
        yield t.srem(TAGS, tag)
        yield t.delete(TAG_SUBSCRIPTIONS_PREFIX.format(tag))
        yield t.delete(TAG_TRIGGERS_PREFIX.format(tag))
        yield t.delete(TAG_PREFIX.format(tag))
        yield t.commit()

    @audit
    @defer.inlineCallbacks
    @docstring_parameters(TRIGGER_PREFIX.format("<trigger_id>"),
                          TRIGGERS, PATTERN_TRIGGERS_PREFIX.format("<pattern>"))
    def removeTrigger(self, trigger_id, existing=None):
        """
        removeTrigger(self, trigger_id)

        Creates redis transaction for:
            - Delete key {0}
            - Remove *trigger_id* from set {1}
            - Remove *trigger_id* from set {2}

        :param trigger_id: trigger identity
        :type trigger_id: string
        """
        if existing is not None:
            t = yield self.rc.multi()
            yield t.delete(TRIGGER_PREFIX.format(trigger_id))
            yield t.delete(TRIGGER_TAGS_PREFIX.format(trigger_id))
            yield t.srem(TRIGGERS, trigger_id)
            for tag in existing.get("tags", []):
                yield t.srem(TAG_TRIGGERS_PREFIX.format(tag), trigger_id)
            for pattern in existing.get("patterns", []):
                yield t.srem(PATTERN_TRIGGERS_PREFIX.format(pattern), trigger_id)
            yield t.commit()
            for pattern in existing.get("patterns", []):
                count = yield self.rc.scard(PATTERN_TRIGGERS_PREFIX.format(pattern))
                if count == 0:
                    yield self.rc.srem(PATTERNS, pattern)
                    for metric in (yield self.getPatternMetrics(pattern)):
                        yield self.rc.delete(METRIC_PREFIX.format(metric))
                    yield self.rc.delete(PATTERN_METRICS_PREFIX.format(pattern))

    @defer.inlineCallbacks
    @docstring_parameters(TRIGGERS)
    def getTriggers(self):
        """
        getTriggers(self)

        Returns all triggers id from set {0}

        :rtype: set of strings
        """
        result = yield self.rc.smembers(TRIGGERS)
        defer.returnValue(result)

    @defer.inlineCallbacks
    @docstring_parameters(LAST_CHECK_PREFIX.format("<trigger_id>"))
    def getTriggerLastCheck(self, trigger_id):
        """
        getTriggerLastCheck(self, trigger_id)

        Returns last trigger check from key {0}

        :param trigger_id: trigger identity
        :type trigger_id: string
        :rtype: json dict
        """
        json = yield self.rc.get(LAST_CHECK_PREFIX.format(trigger_id))
        result = None if json is None else anyjson.deserialize(json)
        defer.returnValue(result)

    @defer.inlineCallbacks
    @docstring_parameters(LAST_CHECK_PREFIX.format("<trigger_id>"))
    def setTriggerLastCheck(self, trigger_id, check):
        """
        setTriggerLastCheck(self, trigger_id, check)

        Save trigger last check to key {0}

        :param trigger_id: trigger identity
        :type trigger_id: string
        :param check: trigger checking result
        :type check: json dict
        """
        json = anyjson.serialize(check)
        t = yield self.rc.multi()
        yield t.set(LAST_CHECK_PREFIX.format(trigger_id), json)
        yield t.zadd(TRIGGERS_CHECKS, check.get("score", 0), trigger_id)
        yield t.incr(CHECKS_COUNTER)
        if check.get("score", 0) > 0:
            yield t.sadd(TRIGGER_IN_BAD_STATE, trigger_id)
        else:
            yield t.srem(TRIGGER_IN_BAD_STATE, trigger_id)
        yield t.commit()

    @defer.inlineCallbacks
    @docstring_parameters(TRIGGER_CHECK_LOCK_PREFIX.format("<trigger_id>"))
    def setTriggerCheckLock(self, trigger_id):
        """
        setTriggerCheckLock(self, trigger_id)

        Try to acquire lock for trigger check {0}

        :param trigger_id: trigger identity
        :type trigger_id: string
        """
        ok = yield self.rc.set(TRIGGER_CHECK_LOCK_PREFIX.format(trigger_id), time.time(),
                               expire=config.CHECK_LOCK_TTL, only_if_not_exists=True)
        defer.returnValue(ok)

    @defer.inlineCallbacks
    def acquireTriggerCheckLock(self, trigger_id, timeout):
        """
        acquireTriggerCheckLock(self, trigger_id, timeout)

        Try to acquire lock for trigger check until timeout

        :param trigger_id: trigger identity
        :type trigger_id: string
        :param timeout: timeout in seconds
        :type timeout: float
        """
        acquired = yield self.setTriggerCheckLock(trigger_id)
        count = 0
        while acquired is None and count < timeout:
            count += 1
            yield task.deferLater(reactor, 0.5, lambda: None)
            acquired = yield self.setTriggerCheckLock(trigger_id)
        if acquired is None:
            raise Exception("Can not acquire trigger lock in {0} seconds".format(timeout))

    @defer.inlineCallbacks
    @docstring_parameters(TRIGGER_CHECK_LOCK_PREFIX.format("<trigger_id>"))
    def delTriggerCheckLock(self, trigger_id):
        """
        delTriggerCheckLock(self, trigger_id)

        Delete lock for trigger check {0}

        :param trigger_id: trigger identity
        :type trigger_id: string
        """
        yield self.rc.delete(TRIGGER_CHECK_LOCK_PREFIX.format(trigger_id))

    @defer.inlineCallbacks
    @docstring_parameters(LAST_CHECK_PREFIX.format("<trigger_id>"))
    def setTriggerMetricsMaintenance(self, trigger_id, metrics, existing=None):
        """
        setTriggerMetricsMaintenance(self, trigger_id, metrics)

        Atomic change of trigger last check and set metric maintenance

        :param trigger_id: trigger identity
        :type trigger_id: string
        :param metrics: metrics maintenance flags
        :type metrics: dict
        """
        key = LAST_CHECK_PREFIX.format(trigger_id)
        json = yield self.rc.get(key)
        while json is not None:
            check = anyjson.deserialize(json)
            metrics_check = check.get("metrics")
            if metrics_check is not None:
                for metric, value in metrics.iteritems():
                    metrics_check[metric]["maintenance"] = value
            prev = yield self.rc.getset(key, anyjson.serialize(check))
            if json == prev:
                break
            json = prev

    @defer.inlineCallbacks
    @docstring_parameters(LAST_CHECK_PREFIX.format("<trigger_id>"))
    def removeTriggerLastCheck(self, trigger_id):
        """
        removeTriggerLastCheck(self, trigger_id)

        Delete trigger last check from key {0}

        :param trigger_id: trigger identity
        :type trigger_id: string
        """
        yield self.rc.delete(LAST_CHECK_PREFIX.format(trigger_id))

    @defer.inlineCallbacks
    @docstring_parameters(PATTERN_METRICS_PREFIX.format("<pattern>"))
    def addPatternMetric(self, pattern, metric):
        """
        addPatternMetric(self, pattern, metric)

        Add metric to set {0}

        :param pattern: pattern of graphite that match multiple metric
        :type pattern: string
        :param metric: metric of graphite
        :type metric: string
        """
        yield self.rc.sadd(PATTERN_METRICS_PREFIX.format(pattern), metric)

    @audit
    @defer.inlineCallbacks
    @docstring_parameters(PATTERN_METRICS_PREFIX.format("<pattern>"))
    def delPatternMetrics(self, pattern, existing=None):
        """
        delPatternMetrics(self, pattern)

        Delete whole set {0} of metrics for given pattern

        :param pattern: pattern of graphite that match multiple metric
        :type pattern: string
        """
        yield self.rc.delete(PATTERN_METRICS_PREFIX.format(pattern))

    @defer.inlineCallbacks
    @docstring_parameters(PATTERN_METRICS_PREFIX.format("<pattern>"))
    def getPatternMetrics(self, pattern):
        """
        getPatternMetrics(self, pattern)

        Read all metrics from set {0} for given pattern

        :param pattern: pattern of graphite that match multiple metric
        :type pattern: string
        :rtype: set of strings
        """
        result = yield self.rc.smembers(PATTERN_METRICS_PREFIX.format(pattern))
        defer.returnValue(result)

    @defer.inlineCallbacks
    @docstring_parameters(METRIC_PREFIX.format("<metric>"))
    def getMetricsValues(self, metrics, startTime, endTime='+inf'):
        """
        getMetricsValues(self, metrics, startTime, endTime)

        Read multiple metric values from sorted set {0} from startTime

        :param metrics: list of graphite metric path
        :type metrics: list of string
        :param startTime: unix epoch time
        :type startTime: long
        :param endTime: unix epoch time
        :type endTime: long
        :rtype: list of list of tuple ('value timestamp', long)
        """
        pipeline = yield self.rc.pipeline()
        for metric in metrics:
            pipeline.zrangebyscore(METRIC_PREFIX.format(metric), min=startTime, max=endTime, withscores=True)
        results = yield pipeline.execute_pipeline()
        defer.returnValue(results)

    @cache
    @defer.inlineCallbacks
    @docstring_parameters(METRIC_PREFIX.format("<metric>"))
    def cleanupMetricValues(self, metric, toTime):
        """
        cleanupMetricValues(self, metric, toTime)

        Remove metric values from sorted set {0} until toTime

        :param startTime: unix epoch time
        :type startTime: long
        """
        yield self.rc.zremrangebyscore(METRIC_PREFIX.format(metric), min="-inf", max=toTime)

    @defer.inlineCallbacks
    @docstring_parameters(PATTERN_TRIGGERS_PREFIX.format("<pattern>"))
    def getPatternTriggers(self, pattern):
        """
        getPatternTriggers(self, pattern)

        Returns all trigger identifiers from set {0}

        :param pattern: pattern of graphite that match multiple metric
        :type pattern: string
        :rtype: set of strings
        """
        result = yield self.rc.smembers(PATTERN_TRIGGERS_PREFIX.format(pattern))
        defer.returnValue(result)

    @defer.inlineCallbacks
    @docstring_parameters(PATTERN_TRIGGERS_PREFIX.format("<pattern>"))
    def removePatternTriggers(self, pattern):
        """
        removePatternTriggers(self, pattern)

        Delete all trigger identifiers from set {0}

        :param pattern: pattern of graphite that match multiple metric
        :type pattern: string
        """
        yield self.rc.delete(PATTERN_TRIGGERS_PREFIX.format(pattern))

    @audit
    @defer.inlineCallbacks
    @docstring_parameters(PATTERNS)
    def removePattern(self, pattern, existing=None):
        """
        removePattern(self, pattern)

        Remove pattern from set {0}

        :param pattern: pattern of graphite that match multiple metric
        :type pattern: string
        """
        yield self.rc.srem(PATTERNS, pattern)

    @audit
    @defer.inlineCallbacks
    @docstring_parameters(EVENTS)
    def pushEvent(self, event, ui=True, existing=None):
        """
        pushEvent(self, event)

        Creates redis transaction for:
            - Add event to beginning of list {0} as json string
            - Trim list to 100 events

        :param event: trigger state changing event
        :type event: dict
        """
        event_json = anyjson.serialize(event)
        t = yield self.rc.multi()
        yield t.lpush(EVENTS, event_json)
        trigger_id = event.get("trigger_id")
        if trigger_id is not None:
            yield t.zadd(TRIGGER_EVENTS.format(event["trigger_id"]), event["timestamp"], event_json)
            yield t.zremrangebyscore(TRIGGER_EVENTS.format(trigger_id),
                                     min="-inf", max=int(time.time() - TRIGGER_EVENTS_TTL))
        if ui:
            yield t.lpush(EVENTS_UI, event_json)
            yield t.ltrim(EVENTS_UI, 0, 100)
        yield t.commit()

    @defer.inlineCallbacks
    @docstring_parameters(EVENTS)
    def getEvents(self, trigger_id=None, start=0, size=100):
        """
        getEvents(self)

        Returns all events for given trigger_id

        :param trigger_id: trigger identity
        :type trigger_id: string
        :rtype: list of dict
        """
        if trigger_id is None:
            events = yield self.rc.lrange(EVENTS_UI, 0, -1)
            total = len(events)
        else:
            pipeline = yield self.rc.pipeline()
            key = TRIGGER_EVENTS.format(trigger_id)
            pipeline.zrevrange(key, start=start, end=(start + size))
            pipeline.zcard(key)
            events, total = yield pipeline.execute_pipeline()
        defer.returnValue(([anyjson.deserialize(e) for e in events], total))

    @defer.inlineCallbacks
    def flush(self):
        yield self.rc.flushdb()

    @defer.inlineCallbacks
    @docstring_parameters(METRIC_PREFIX.format("<metric>"))
    def delMetric(self, metric):
        """
        delMetric(self, metric)

        Delete metric sorted set {0}

        :param metric: metric of graphite
        :type metric: string
        """
        key = METRIC_PREFIX.format(metric)
        yield self.rc.delete(key)

    @cache
    @defer.inlineCallbacks
    @docstring_parameters(METRIC_RETENTION_PREFIX.format("<metric>"))
    def getMetricRetention(self, metric):
        """
        getMetricRetention(self, metric)

        Returns metric retention in seconds from key {0} for given metric

        :param metric: metric of graphite
        :type metric: string
        :rtype: integer
        """
        key = METRIC_RETENTION_PREFIX.format(metric)
        result = yield self.rc.get(key)
        defer.returnValue(60 if result is None else int(result))

    @defer.inlineCallbacks
    def sendMetric(self, pattern, metric, timestamp, value):
        key = METRIC_PREFIX.format(metric)
        yield self.rc.zadd(key, timestamp, "{0} {1}".format(timestamp, value))
        yield self.addPatternMetric(pattern, metric)

    @defer.inlineCallbacks
    def join(self, keys, function, result):
        for key in keys:
            item = yield function(key)
            if item is not None:
                result.append(item)

    @defer.inlineCallbacks
    def stopService(self):
        """
        stopService(self)

        Disconnect from redis connection pool
        """
        if self.rc:
            yield self.rc.disconnect()
