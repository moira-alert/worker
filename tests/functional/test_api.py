import anyjson
from . import trigger, WorkerTests, BodyReceiver
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.web import http, client
from twisted.web.http_headers import Headers
from twisted.python.log import ILogObserver
from StringIO import StringIO
from moira.checker import state, worker
from moira import logs


class ApiTests(WorkerTests):

    @inlineCallbacks
    def request(self, method, url, content=None, state=http.OK):
        body = None if content is None else client.FileBodyProducer(
            StringIO(content))
        headers = Headers({'Content-Type': ['application/json'],
                           'X-WebAuth-User': ['tester']})
        response = yield self.client.request(method,
                                             self.url_prefix + url,
                                             headers,
                                             body)
        self.assertEqual(state, response.code)
        body_receiver = BodyReceiver()
        response.deliverBody(body_receiver)
        body = yield body_receiver.finished
        if response.headers.getRawHeaders('content-type') == ['application/json']:
            body = anyjson.loads(body)
        returnValue((response, body))

    @trigger("not-existing")
    @inlineCallbacks
    def testTriggerNotFound(self):
        response, body = yield self.request('GET', 'trigger/{0}'.format(self.trigger.id), state=http.NOT_FOUND)

    @trigger("throttling")
    @inlineCallbacks
    def testThrottling(self):
        response, body = yield self.request('PUT', 'trigger/{0}'.format(self.trigger.id),
                                            '{"name": "test trigger", "targets": ["DevOps.Metric"], \
                                             "warn_value": "1e-7", "error_value": 50, "tags": ["tag1", "tag2"] }',
                                            )
        yield self.db.setTriggerThrottling(self.trigger.id, self.now + 3600)
        yield self.db.addThrottledEvent(self.trigger.id, self.now + 3600, {'trigger_id': self.trigger.id})
        response, json = yield self.request('GET', 'trigger/{0}/throttling'.format(self.trigger.id))
        self.assertTrue(json['throttling'])
        response, json = yield self.request('DELETE', 'trigger/{0}/throttling'.format(self.trigger.id))
        response, json = yield self.request('GET', 'trigger/{0}/throttling'.format(self.trigger.id))
        self.assertFalse(json['throttling'])

    @inlineCallbacks
    def testPatternCleanup(self):
        response, body = yield self.request('PUT', 'trigger/name',
                                            '{"targets": ["DevOps.*.Metric"], \
                                             "warn_value": 1, "error_value": 2}')
        patterns = yield self.db.getPatterns()
        self.assertEqual(list(patterns), ["DevOps.*.Metric"])
        response, body = yield self.request('PUT', 'trigger/name',
                                            '{"targets": ["DevOps.*.OtherMetric"], \
                                             "warn_value": 1, "error_value": 2}')
        patterns = yield self.db.getPatterns()
        self.assertEqual(list(patterns), ["DevOps.*.OtherMetric"])
        response, body = yield self.request('DELETE', 'trigger/name')
        patterns = yield self.db.getPatterns()
        self.assertEqual(len(patterns), 0)

    @trigger("delete-tag")
    @inlineCallbacks
    def testTagDeletion(self):
        response, body = yield self.request('PUT', 'trigger/{0}'.format(self.trigger.id),
                                            '{"name": "test trigger", "targets": ["sumSeries(*)"], \
                                             "warn_value": "1e-7", "error_value": 50, "tags": ["tag1", "tag2"] }',
                                            )
        response, body = yield self.request('GET', 'trigger/{0}'.format(self.trigger.id))
        response, body = yield self.request('DELETE', 'tag/tag1', state=http.BAD_REQUEST)
        response, body = yield self.request('DELETE', 'trigger/{0}'.format(self.trigger.id))
        response, body = yield self.request('DELETE', 'tag/tag1')

    @trigger("good-trigger")
    @inlineCallbacks
    def testSimpleTriggerPUT(self):
        response, body = yield self.request('PUT', 'trigger/{0}'.format(self.trigger.id),
                                            '{"name": "test trigger", "targets": ["sumSeries(*)"], \
                                             "warn_value": "1e-7", "error_value": 50, "tags": ["tag1", "tag2"] }',
                                            )
        response, tags = yield self.request('GET', 'tag/stats')
        response, patterns = yield self.request('GET', 'pattern')
        self.assertEqual(2, len(tags["list"]))
        self.assertEqual(1, len(patterns["list"]))
        self.assertEqual(self.trigger.id, patterns["list"][0]["triggers"][0]["id"])
        response, triggers = yield self.request('GET', 'trigger')
        self.assertEqual(1, len(triggers["list"]))

    @trigger("expression-trigger")
    @inlineCallbacks
    def testExpressionTriggerPUT(self):
        response, body = yield self.request('PUT', 'trigger/{0}'.format(self.trigger.id),
                                            '{"name": "test trigger", "targets": ["sumSeries(*)"], \
                                             "tags": ["tag1", "tag2"], "expression": "ERROR if t1 > 1 else OK" }',
                                            )
        response, triggers = yield self.request('GET', 'trigger')
        self.assertEqual(1, len(triggers["list"]))

    @trigger("not-json-trigger")
    @inlineCallbacks
    def testSendNotJsonTrigger(self):
        response, body = yield self.request('PUT', 'trigger/{0}'.format(self.trigger.id),
                                            "i am not json", http.BAD_REQUEST)
        self.flushLoggedErrors()
        self.assertEqual("Content is not json", body)

    @trigger("invalid-expression-trigger")
    @inlineCallbacks
    def testSendInvalidExpressionTrigger(self):
        response, body = yield self.request('PUT', 'trigger/{0}'.format(self.trigger.id),
                                            '{"name":"test trigger","targets":["metric"], \
                                             "warn_value":-0.1, "error_value":0.1,"ttl":600,"ttl_state":"NODATA", \
                                             "tags":["tag1"],"expression":"ERROR if"}', http.BAD_REQUEST)
        self.flushLoggedErrors()
        self.assertEqual("Invalid expression", body)

    @trigger("wrong-time-span")
    @inlineCallbacks
    def testSendWrongTimeSpan(self):
        response, body = yield self.request('PUT', 'trigger/{0}'.format(self.trigger.id),
                                            '{"name": "test trigger", "targets": ["movingAverage(*, \\"10m\\")"], \
                                             "warn_value": "1e-7", "error_value": 50}', http.BAD_REQUEST)
        self.flushLoggedErrors()
        self.assertEqual("Invalid graphite target", body)

    @trigger("without-warn-value")
    @inlineCallbacks
    def testSendWithoutWarnValue(self):
        response, body = yield self.request('PUT', 'trigger/{0}'.format(self.trigger.id),
                                            '{"name": "test trigger", "targets": ["sumSeries(*)"], "error_value": 50 }',
                                            http.BAD_REQUEST)
        self.flushLoggedErrors()
        self.assertEqual("warn_value is required", body)

    @trigger("test-events")
    @inlineCallbacks
    def testEvents(self):
        yield self.db.pushEvent({
            "trigger_id": self.trigger.id,
            "state": state.OK,
            "old_state": state.WARN,
            "timestamp": self.now - 120,
            "metric": "test metric"
        })
        yield self.db.pushEvent({
            "trigger_id": self.trigger.id,
            "state": state.WARN,
            "old_state": state.OK,
            "timestamp": self.now,
            "metric": "test metric"
        })
        response, events = yield self.request('GET', 'event/{0}'.format(self.trigger.id))
        self.assertEqual(2, len(events['list']))
        response, events = yield self.request('GET', 'event')
        self.assertEqual(2, len(events['list']))

    @inlineCallbacks
    def testUserContact(self):
        contact = {'value': 'tester@company.com',
                   'type': 'email'}
        response, saved = yield self.request('PUT', 'contact', anyjson.dumps(contact))
        contact['id'] = saved['id']
        contact['user'] = 'tester'
        self.assertEqual(contact, saved)
        response, settings = yield self.request('GET', 'user/settings')
        self.assertEqual([contact], settings["contacts"])
        response, settings = yield self.request('GET', 'user/settings')
        self.assertEqual(contact['id'], settings["contacts"][0]["id"])
        response, body = yield self.request('DELETE', 'contact/' + str(contact['id']))
        response, settings = yield self.request('GET', 'user/settings')
        self.assertEqual([], settings["contacts"])

    @inlineCallbacks
    def testUserSubscriptions(self):
        contact = {'value': 'tester@company.com',
                   'type': 'email'}
        response, contact = yield self.request('PUT', 'contact', anyjson.dumps(contact))
        response, sub = yield self.request('PUT', 'subscription', anyjson.dumps({
            "contacts": [contact["id"]],
            "tags": ["devops", "tag1"]
        }))
        response, body = yield self.request('PUT', 'subscription/' + str(sub["id"]) + "/test")
        response, subscriptions = yield self.request('GET', 'subscription')
        self.assertEqual(sub['id'], subscriptions["list"][0]["id"])
        response, settings = yield self.request('GET', 'user/settings')
        self.assertEqual(sub['id'], settings["subscriptions"][0]["id"])
        subs = yield self.db.getTagSubscriptions("devops")
        self.assertEqual(sub["id"], subs[0]["id"])
        subs = yield self.db.getTagSubscriptions("tag1")
        self.assertEqual(sub["id"], subs[0]["id"])
        sub["tags"].remove("tag1")
        response, updated_sub = yield self.request('PUT', 'subscription', anyjson.serialize(sub))
        subs = yield self.db.getTagSubscriptions("tag1")
        self.assertEqual(len(subs), 0)
        response, updated_sub = yield self.request('DELETE', 'subscription/' + str(sub["id"]))
        subs = yield self.db.getTagSubscriptions("devops")
        self.assertEqual(len(subs), 0)

    @inlineCallbacks
    def testUserContactDelete(self):
        contact = {'value': 'tester@company.com',
                   'type': 'email'}
        response, contact = yield self.request('PUT', 'contact', anyjson.dumps(contact))
        response, sub = yield self.request('PUT', 'subscription', anyjson.dumps({
            "contacts": [contact["id"]],
            "tags": ["devops", "tag1"]
        }))
        response, body = yield self.request('PUT', 'subscription/' + str(sub["id"]) + "/test")
        response, body = yield self.request('DELETE', 'contact/' + str(contact["id"]))
        response, subscriptions = yield self.request('GET', 'subscription')
        self.assertNotIn(contact['id'], subscriptions["list"][0]["contacts"])

    @trigger("test-metrics")
    @inlineCallbacks
    def testMetricDeletion(self):
        pattern = "devops.functest.*"
        metric1 = "devops.functest.m1"
        metric2 = "devops.functest.m2"
        yield self.db.sendMetric(pattern, metric1, self.now - 60, 1)
        yield self.db.sendMetric(pattern, metric1, self.now, 2)
        yield self.db.sendMetric(pattern, metric2, self.now, 3)
        response, body = yield self.request('PUT', 'trigger/{0}'.format(self.trigger.id),
                                            '{"name": "test trigger", "targets": ["' + pattern + '"], \
                                             "warn_value": 5, "error_value": 10 }',
                                            )
        response, metrics = yield self.request('GET', 'trigger/{0}/metrics?from={1}&to={2}'
                                               .format(self.trigger.id, self.now - 60, self.now))
        self.assertEqual(2, len(metrics))
        self.assertEqual([1, 2], [v['value'] for v in metrics[metric1]])
        metrics = yield self.db.getPatternMetrics(pattern)
        self.assertTrue(metric1 in metrics)
        self.assertTrue(metric2 in metrics)
        yield self.trigger.check()
        check = yield self.db.getTriggerLastCheck(self.trigger.id)
        self.assertEqual(2, len(check['metrics']))
        response, data = yield self.request('DELETE', 'trigger/{0}/metrics?name={1}'
                                            .format(self.trigger.id, metric1))
        metrics = yield self.db.getPatternMetrics(pattern)
        self.assertFalse(metric1 in metrics)
        self.assertFalse(metric2 in metrics)
        check = yield self.db.getTriggerLastCheck(self.trigger.id)
        self.assertEqual(1, len(check['metrics']))

    @trigger("test-trigger-maintenance")
    @inlineCallbacks
    def testTriggerMaintenance(self):
        metric = "devops.functest.m"
        yield self.db.sendMetric(metric, metric, self.now - 60, 0)
        response, body = yield self.request('PUT', 'trigger/{0}'.format(self.trigger.id),
                                            '{"name": "test trigger", "targets": ["' + metric + '"], \
                                             "warn_value": 0, "error_value": 1, "tags":["tag1"] }',
                                            )
        response, _ = yield self.request('PUT', 'tag/tag1/data', anyjson.dumps({"maintenance": self.now}))
        yield self.trigger.check(now=self.now - 1)
        events = yield self.db.getEvents()
        self.assertEqual(0, len(events))
        response, _ = yield self.request('PUT', 'tag/tag1/data', anyjson.dumps({}))
        yield self.db.sendMetric(metric, metric, self.now, 1)
        yield self.trigger.check()
        events = yield self.db.getEvents()
        self.assertEqual(1, len(events))

    @trigger("test-trigger-maintenance2")
    @inlineCallbacks
    def testTriggerMaintenance2(self):
        metric = "devops.functest.m"
        yield self.db.sendMetric(metric, metric, self.now - 60, 1)
        response, body = yield self.request('PUT', 'trigger/{0}'.format(self.trigger.id),
                                            '{"name": "test trigger", "targets": ["' + metric + '"], \
                                             "warn_value": 1, "error_value": 2, "tags":["tag1"] }',
                                            )
        response, _ = yield self.request('PUT', 'tag/tag1/data', anyjson.dumps({"maintenance": self.now}))
        yield self.trigger.check(now=self.now - 1)
        events = yield self.db.getEvents()
        self.assertEqual(0, len(events))
        response, _ = yield self.request('PUT', 'tag/tag1/data', anyjson.dumps({}))
        yield self.db.sendMetric(metric, metric, self.now, 0)
        yield self.db.sendMetric(metric, metric, self.now + 60, 1)
        yield self.db.sendMetric(metric, metric, self.now + 120, 1)
        yield self.trigger.check(now=self.now + 120)
        yield self.assert_trigger_metric(metric, 1, state.WARN)
        events = yield self.db.getEvents()
        self.assertEqual(2, len(events))

    @trigger("test-metric-maintenance")
    @inlineCallbacks
    def testMetricMaintenance(self):
        metric = "devops.functest.m"
        yield self.db.sendMetric(metric, metric, self.now - 60, 0)
        response, body = yield self.request('PUT', 'trigger/{0}'.format(self.trigger.id),
                                            '{"name": "test trigger", "targets": ["' + metric + '"], \
                                             "warn_value": 0, "error_value": 1, "tags":["tag1"] }',
                                            )
        yield self.trigger.check(now=self.now - 1)
        events = yield self.db.getEvents()
        self.assertEqual(1, len(events))
        response, _ = yield self.request('PUT', 'trigger/{0}/maintenance'.format(self.trigger.id), anyjson.dumps({metric: self.now}))
        yield self.db.sendMetric(metric, metric, self.now, 1)
        yield self.trigger.check()
        events = yield self.db.getEvents()
        self.assertEqual(1, len(events))
        yield self.db.sendMetric(metric, metric, self.now + 60, -1)
        yield self.trigger.check(now=self.now + 60)
        events = yield self.db.getEvents()
        self.assertEqual(2, len(events))

    @inlineCallbacks
    def testUserLogin(self):
        response, user = yield self.request('GET', 'user')
        self.assertEqual('tester', user["login"])
