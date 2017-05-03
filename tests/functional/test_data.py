import anyjson

from moira.graphite import datalib
from . import trigger, WorkerTests
from StringIO import StringIO
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.task import deferLater
from twisted.internet import reactor
from twisted.web import http, client
from twisted.web.http_headers import Headers
from moira.checker import state, worker
from moira.checker.worker import TriggersCheck


class DataTests(WorkerTests):
    @inlineCallbacks
    def sendTrigger(self, trigger):
        body = client.FileBodyProducer(StringIO(trigger))
        response = yield self.client.request('PUT', self.url_prefix + 'trigger/{0}'.format(self.trigger.id),
                                             Headers({'Content-Type': ['application/json']}), body)
        self.assertEqual(http.OK, response.code)
        returnValue(anyjson.loads(trigger))

    @inlineCallbacks
    def deleteTrigger(self):
        response = yield self.client.request('DELETE', self.url_prefix + 'trigger/{0}'.format(self.trigger.id))
        self.assertEqual(http.OK, response.code)

    @trigger("bad-trigger")
    @inlineCallbacks
    def testTriggersCheckService(self):
        service = TriggersCheck(self.db)
        service.start()
        yield self.db.saveTrigger(self.trigger.id, {})
        yield self.db.addTriggerCheck(self.trigger.id)
        worker.ERROR_TIMEOUT = 0.01
        yield deferLater(reactor, worker.PERFORM_INTERVAL * 2, lambda: None)
        self.flushLoggedErrors()
        yield service.stop()

    @trigger("test-trigger-sum-series")
    @inlineCallbacks
    def testSumSeries(self):
        pattern = 'MoiraFuncTest.supervisord.host.*.state'
        metric1 = 'MoiraFuncTest.supervisord.host.one.state'
        metric2 = 'MoiraFuncTest.supervisord.host.two.state'
        metric3 = 'MoiraFuncTest.supervisord.host.three.state'
        trigger = yield self.sendTrigger('{"name": "test trigger", "targets": ["sumSeries(' + pattern +
                                         ')"], "warn_value": 20, "error_value": 50, "ttl":"600" }')
        yield self.db.sendMetric(pattern, metric1, self.now - 1, 1)
        yield self.db.sendMetric(pattern, metric2, self.now - 1, 2)
        yield self.db.sendMetric(pattern, metric3, self.now - 1, 3)
        yield self.protocol.messageReceived(None, "moira-func-test", '{"pattern":"' + pattern +
                                            '", "metric":"' + metric3 + '"}')
        yield self.check.perform()
        yield self.assert_trigger_metric(trigger["targets"][0], 6, state.OK)

    @trigger("test-trigger-exception")
    @inlineCallbacks
    def testTriggerException(self):
        yield self.sendTrigger('{"name": "test trigger", "targets": ["m1", "m2"],\
                                 "expression":"ERROR if t1/t2 else OK"}')
        yield self.db.sendMetric("m1", "m1", self.now - 1, 0)
        yield self.db.sendMetric("m2", "m2", self.now - 1, 0)
        yield self.trigger.check()
        self.flushLoggedErrors()
        events, total = yield self.db.getEvents()
        self.assertEquals(1, total)
        self.assertEquals(events[0]['state'], state.EXCEPTION)

    @trigger("test-trigger-exception-multi-series")
    @inlineCallbacks
    def testTriggerMultiSeriesException(self):
        yield self.sendTrigger('{"name": "test trigger", "targets": ["m1", "m2*"],\
                                 "expression":"ERROR if t1/t2 else OK"}')
        yield self.db.sendMetric("m1", "m1", self.now - 1, 1)
        yield self.db.sendMetric("m2*", "m2.1", self.now - 1, 1)
        yield self.db.sendMetric("m2*", "m2.2", self.now - 1, 2)
        yield self.trigger.check()
        self.flushLoggedErrors()
        events, total = yield self.db.getEvents()
        self.assertEquals(1, total)
        self.assertEquals(events[0]['state'], state.EXCEPTION)

    @trigger('test-trigger-patterns')
    @inlineCallbacks
    def testTriggerPatterns(self):
        pattern = 'MoiraFuncTest.supervisord.host.{4*,5*}.state'
        metric1 = 'MoiraFuncTest.supervisord.host.400.state'
        metric2 = 'MoiraFuncTest.supervisord.host.500.state'
        yield self.sendTrigger('{"name": "test trigger", "targets": ["movingAverage(' +
                               pattern + ',10)"], "warn_value": 20, "error_value": 50, "ttl":"600" }')
        json, trigger = yield self.db.getTrigger(self.trigger.id)
        self.assertEquals([pattern], trigger["patterns"])
        yield self.db.sendMetric(pattern, metric1, self.now, 1)
        yield self.db.sendMetric(pattern, metric2, self.now, 1)
        yield self.db.sendMetric(pattern, metric1, self.now - 60, 1)
        yield self.db.sendMetric(pattern, metric2, self.now - 60, 1)
        yield self.trigger.check()
        yield self.assert_trigger_metric('movingAverage(' +
                                         metric1 + ',10)', 1, state.OK)
        yield self.assert_trigger_metric('movingAverage(' +
                                         metric2 + ',10)', 1, state.OK)
        yield self.sendTrigger('{"name": "test trigger", "targets": ["movingAverage(' +
                               pattern + ',10)"], "warn_value": 20, "error_value": 50, "ttl":"600" }')
        json, trigger = yield self.db.getTrigger(self.trigger.id)
        self.assertEquals([pattern], trigger["patterns"])

    @trigger('test-trigger-expression')
    @inlineCallbacks
    def testTriggerExpression(self):
        metric1 = 'MoiraFuncTest.one'
        metric2 = 'MoiraFuncTest.two'
        yield self.sendTrigger('{"name": "test trigger", "targets": ["' + metric1 + '", \
                               "' + metric2 + '"], \
                               "expression": "ERROR if t1 > t2 else OK", \
                               "ttl":"600" }')
        json, trigger = yield self.db.getTrigger(self.trigger.id)
        yield self.db.sendMetric(metric1, metric1, self.now - 60, 1)
        yield self.db.sendMetric(metric2, metric2, self.now - 60, 2)
        yield self.trigger.check(now=self.now)
        yield self.assert_trigger_metric(metric1, 1, state.OK)
        yield self.assert_trigger_metric(metric2, 2, state.OK)
        yield self.db.sendMetric(metric1, metric1, self.now, 4)
        yield self.db.sendMetric(metric2, metric2, self.now, 3)
        yield self.trigger.check(now=self.now + 60)
        yield self.assert_trigger_metric(metric1, 4, state.ERROR)
        yield self.assert_trigger_metric(metric2, 3, state.ERROR)

    @trigger('test-trigger-expression-prev-state')
    @inlineCallbacks
    def testPrevStateTriggerExpression(self):
        metric1 = 'MoiraFuncTest.one'
        metric2 = 'MoiraFuncTest.two'
        yield self.sendTrigger('{"name": "test trigger", "targets": ["' + metric1 + '", \
                               "' + metric2 + '"], \
                               "expression": "ERROR if t1 > 10 else PREV_STATE if t2 > 0 else OK", \
                               "ttl":"600" }')
        json, trigger = yield self.db.getTrigger(self.trigger.id)
        yield self.db.sendMetric(metric1, metric1, self.now - 120, 10)
        yield self.db.sendMetric(metric2, metric2, self.now - 120, 0)
        yield self.trigger.check(now=self.now)
        yield self.assert_trigger_metric(metric1, 10, state.OK)
        yield self.db.sendMetric(metric1, metric1, self.now - 60, 11)
        yield self.db.sendMetric(metric2, metric2, self.now - 60, 1)
        yield self.trigger.check(now=self.now)
        yield self.assert_trigger_metric(metric1, 11, state.ERROR)
        yield self.db.sendMetric(metric1, metric1, self.now, 9)
        yield self.db.sendMetric(metric2, metric2, self.now, 1)
        yield self.trigger.check(now=self.now + 60)
        yield self.assert_trigger_metric(metric1, 9, state.ERROR)

    @trigger('test-trigger-patterns2')
    @inlineCallbacks
    def testTriggerPatterns2(self):
        metric = 'MoiraFuncTest.supervisord.host.state'
        yield self.sendTrigger('{"name": "test trigger", "targets": ["movingAverage(' +
                               metric + ', 10)"], "warn_value": 20, "error_value": 50, "ttl":"600" }')
        json, trigger = yield self.db.getTrigger(self.trigger.id)
        self.assertEquals([metric], trigger["patterns"])
        yield self.db.sendMetric(metric, metric, self.now - 60, 1)
        yield self.trigger.check()
        yield self.assert_trigger_metric('movingAverage(' +
                                         metric + ',10)', 1, state.OK)
        yield self.sendTrigger('{"name": "test trigger", "targets": ["movingAverage(transformNull(' +
                               metric + ', 0), 10)"], "warn_value": 20, "error_value": 50, "ttl":"600" }')
        json, trigger = yield self.db.getTrigger(self.trigger.id)
        self.assertEquals([metric], trigger["patterns"])

    @trigger('test-trigger-patterns3')
    @inlineCallbacks
    def testTriggerPatterns3(self):
        pattern = 'MoiraFuncTest.supervisord.*.*.state.node'
        yield self.sendTrigger('{"name": "test trigger", "targets": ["movingAverage(groupByNode(' +
                               pattern + ',2,\'maxSeries\'),10)"],' +
                               '"warn_value": 20, "error_value": 50, "ttl":"600" }')
        json, trigger = yield self.db.getTrigger(self.trigger.id)
        self.assertEquals([pattern], trigger["patterns"])

    @trigger('test-trigger-exclude')
    @inlineCallbacks
    def testExclude(self):
        pattern = 'MoiraFuncTest.supervisord.host.*.state'
        metric1 = 'MoiraFuncTest.supervisord.host.one.state'
        metric2 = 'MoiraFuncTest.supervisord.host.two.state'
        yield self.sendTrigger('{"name": "test trigger", "targets": ["exclude(' + pattern +
                               ', \'two\')"], "warn_value": 20, "error_value": 50, "ttl":"600" }')
        yield self.db.sendMetric(pattern, metric1, self.now - 60, 1)
        yield self.db.sendMetric(pattern, metric2, self.now - 60, 60)
        yield self.trigger.check()
        yield self.assert_trigger_metric(metric1, 1, state.OK)
        yield self.assert_trigger_metric(metric2, None, None)

    @trigger('test-trigger-exclude-with-moving-average')
    @inlineCallbacks
    def testExcludeWithMovingAverage(self):
        pattern = 'MoiraFuncTest.supervisord.host.*.state'
        metric1 = 'MoiraFuncTest.supervisord.host.one.state'
        metric2 = 'MoiraFuncTest.supervisord.host.two.state'
        yield self.sendTrigger('{"name": "test trigger", "targets": ["exclude(movingAverage(' + pattern +
                               ', 3), \'two\')"], "warn_value": 20, "error_value": 30, "ttl":"600" }')

        yield self.db.sendMetric(pattern, metric1, self.now - 180, 10)
        yield self.db.sendMetric(pattern, metric1, self.now - 120, 20)
        yield self.db.sendMetric(pattern, metric1, self.now - 60, 30)

        yield self.db.sendMetric(pattern, metric2, self.now - 180, 30)
        yield self.db.sendMetric(pattern, metric2, self.now - 120, 40)
        yield self.db.sendMetric(pattern, metric2, self.now - 60, 50)

        yield self.trigger.check(now=self.now)
        yield self.assert_trigger_metric('movingAverage(' + metric1 + ',3)', 20, state.WARN)
        yield self.assert_trigger_metric('movingAverage(' + metric2 + ',3)', None, None)

    @trigger('test-trigger-moving-average-with-exclude')
    @inlineCallbacks
    def testMovingAverageWithExclude(self):
        pattern = 'MoiraFuncTest.supervisord.host.*.state'
        metric1 = 'MoiraFuncTest.supervisord.host.one.state'
        metric2 = 'MoiraFuncTest.supervisord.host.two.state'
        yield self.sendTrigger('{"name": "test trigger", "targets": ["movingAverage(exclude(' + pattern +
                               ', \'two\'), 3)"], "warn_value": 20, "error_value": 30, "ttl":"600" }')

        yield self.db.sendMetric(pattern, metric1, self.now - 180, 10)
        yield self.db.sendMetric(pattern, metric1, self.now - 120, 20)
        yield self.db.sendMetric(pattern, metric1, self.now - 60, 30)

        yield self.db.sendMetric(pattern, metric2, self.now - 180, 30)
        yield self.db.sendMetric(pattern, metric2, self.now - 120, 40)
        yield self.db.sendMetric(pattern, metric2, self.now - 60, 50)

        yield self.trigger.check(now=self.now)
        yield self.assert_trigger_metric('movingAverage(' + metric1 + ',3)', 20, state.WARN)
        yield self.assert_trigger_metric('movingAverage(' + metric2 + ',3)', None, None)

    @trigger('test-trigger-transformNull')
    @inlineCallbacks
    def testTransformNull(self):
        pattern = 'MoiraFuncTest.supervisord.host.*.state'
        metric1 = 'MoiraFuncTest.supervisord.host.one.state'
        yield self.sendTrigger(trigger='{"name": "test trigger", "targets": ["movingAverage(transformNull(' + pattern +
                                       ', 0),2)"], "warn_value": 20, "error_value": 50, "ttl":"600" }')
        yield self.db.sendMetric(pattern, metric1, self.now - 180, 5)
        yield self.db.sendMetric(pattern, metric1, self.now - 60, 5)
        yield self.trigger.check()
        yield self.assert_trigger_metric('movingAverage(transformNull(' + metric1 + ',0),2)', 2.5, state.OK)

    @trigger('test-trigger-alias-max')
    @inlineCallbacks
    def testAliasMaxSeries(self):
        pattern = 'MoiraFuncTest.supervisord.host.*.state'
        metric1 = 'MoiraFuncTest.supervisord.host.one.state'
        metric2 = 'MoiraFuncTest.supervisord.host.two.state'
        metric3 = 'MoiraFuncTest.supervisord.host.three.state'
        yield self.sendTrigger('{"name": "test trigger", "targets": ["alias(maxSeries(' + pattern +
                               '), \'node\')"],  "warn_value": 20, "error_value": 50, "ttl":"600" }')
        yield self.db.sendMetric(pattern, metric1, self.now - 5, 0)
        yield self.db.sendMetric(pattern, metric2, self.now - 5, 10)
        yield self.db.sendMetric(pattern, metric3, self.now - 5, 80)
        yield self.protocol.messageReceived(None, "moira-func-test", '{"pattern":"' + pattern +
                                            '", "metric":"' + metric3 + '"}')
        yield self.check.perform()
        yield self.assert_trigger_metric('node', 80, state.ERROR)

    @trigger('test-trigger-summarize-sum')
    @inlineCallbacks
    def testSummarizeSum(self):
        pattern = 'MoiraFuncTest.supervisord.host.*.state'
        metric = 'MoiraFuncTest.supervisord.host.one.state'
        yield self.sendTrigger('{"name": "test trigger", "targets": ["summarize(' + pattern +
                               ', \'10min\', \'sum\', false)"],  "warn_value": 20, "error_value": 50, "ttl":"3600" }')
        begin = self.now - self.now % 3600
        yield self.db.sendMetric(pattern, metric, begin, 10)
        yield self.db.sendMetric(pattern, metric, begin + 60, 20)
        yield self.db.sendMetric(pattern, metric, begin + 120, 30)
        yield self.trigger.check(fromTime=begin, now=begin + 180)
        yield self.assert_trigger_metric('summarize(' + metric + ', "10min", "sum")', 60, state.ERROR)

    @trigger('test-trigger-summarize-min')
    @inlineCallbacks
    def testSummarizeMin(self):
        metric = 'MoiraFuncTest.supervisord.host.one.state'
        yield self.sendTrigger('{"name": "test trigger", "targets": ["summarize(' + metric +
                               ', \'10min\', \'min\')"],  "warn_value": 0.001, "error_value": 50, "ttl":"3600" }')
        begin = self.now - self.now % 600
        yield self.db.sendMetric(metric, metric, begin + 1, 10)
        yield self.trigger.check(fromTime=begin, now=begin + 600)
        yield self.assert_trigger_metric('summarize(' + metric +
                                         ', "10min", "min")', 10, state.WARN)

    @trigger('test-trigger-aliasbynode')
    @inlineCallbacks
    def testAliasByNode(self):
        pattern = 'MoiraFuncTest.supervisord.host.*.state'
        metric1 = 'MoiraFuncTest.supervisord.host.one.state'
        yield self.sendTrigger('{"name": "test trigger", "targets": ["aliasByNode(' + pattern +
                               ', 3)"],  "warn_value": 20, "error_value": 50, "ttl":"600" }')
        yield self.db.sendMetric(pattern, metric1, self.now - 60, 30)
        yield self.trigger.check()
        yield self.assert_trigger_metric('one', 30, state.WARN)

    @trigger('test-trigger-aliasbynode-nometrics')
    @inlineCallbacks
    def testAliasByNodeWithNoMetrics(self):
        yield self.sendTrigger('{"name": "test trigger", "targets": ["aliasByNode( \
                               movingAverage(m{1,2}, \'5min\'), 0)"], \
                               "warn_value": 20, "error_value": 50, "ttl":"600" }')
        yield self.trigger.check()
        check = yield self.db.getTriggerLastCheck(self.trigger.id)
        yield self.assert_trigger_metric('m2', None, state.NODATA)

    @trigger('test-trigger-group')
    @inlineCallbacks
    def testGroupBy(self):
        pattern = 'MoiraFuncTest.supervisord.host.*.state'
        metric1 = 'MoiraFuncTest.supervisord.host.one.state'
        metric2 = 'MoiraFuncTest.supervisord.host.two.state'
        metric3 = 'MoiraFuncTest.supervisord.host.three.state'
        yield self.sendTrigger('{"name": "test trigger", "targets": ["groupByNode(' + pattern +
                               ',4,\'averageSeries\')"],  "warn_value": 20, "error_value": 50, "ttl":"600" }')
        yield self.db.sendMetric(pattern, metric1, self.now - 1, 0)
        yield self.db.sendMetric(pattern, metric2, self.now - 1, 10)
        yield self.db.sendMetric(pattern, metric3, self.now - 1, 80)
        yield self.trigger.check()
        yield self.assert_trigger_metric('state', 30, state.WARN)

    @trigger('test-trigger-min-series')
    @inlineCallbacks
    def testMinSeries(self):
        pattern = 'MoiraFuncTest.supervisord.host.*.state'
        metric1 = 'MoiraFuncTest.supervisord.host.one.state'
        metric2 = 'MoiraFuncTest.supervisord.host.two.state'
        metric3 = 'MoiraFuncTest.supervisord.host.three.state'
        yield self.sendTrigger('{"name": "test trigger", "targets": ["minSeries(' + pattern +
                               ')"],  "warn_value": 20, "error_value": 50, "ttl":"600" }')
        yield self.db.sendMetric(pattern, metric1, self.now - 1, 5)
        yield self.db.sendMetric(pattern, metric2, self.now - 1, 10)
        yield self.db.sendMetric(pattern, metric3, self.now - 1, 80)
        yield self.trigger.check()
        yield self.assert_trigger_metric('minSeries(' + pattern +
                                         ')', 5, state.OK)

    @trigger('test-trigger-moving-average')
    @inlineCallbacks
    def testMovingAverage(self):
        metric = 'MoiraFuncTest.system.test.physicaldisk.one-drive.diskqueuelength'
        yield self.sendTrigger('{"name": "test trigger", "targets": ["movingAverage(' + metric +
                               ',3)"],  "warn_value": 20, "error_value": 30, "ttl":"600" }')
        yield self.db.sendMetric(metric, metric, self.now - 180, 10)
        yield self.db.sendMetric(metric, metric, self.now - 120, 20)
        yield self.db.sendMetric(metric, metric, self.now - 60, 30)
        yield self.trigger.check(now=self.now - 60)
        yield self.assert_trigger_metric('movingAverage(' + metric +
                                         ',3)', 20, state.WARN)
        yield self.db.sendMetric(metric, metric, self.now, 40)
        yield self.trigger.check(now=self.now)
        yield self.assert_trigger_metric('movingAverage(' + metric +
                                         ',3)', 30, state.ERROR)

    @trigger('test-trigger-moving-average-min')
    @inlineCallbacks
    def testMovingAverageMin(self):
        metric = 'MoiraFuncTest.system.test.physicaldisk.one-drive.diskqueuelength'
        yield self.sendTrigger('{"name": "test trigger", "targets": ["movingAverage(' + metric +
                               ',3, \\"min\\")"],  "warn_value": 20, "error_value": 30, "ttl":"600" }')
        yield self.db.sendMetric(metric, metric, self.now - 180, 10)
        yield self.db.sendMetric(metric, metric, self.now - 120, 20)
        yield self.db.sendMetric(metric, metric, self.now - 60, 30)
        yield self.trigger.check(now=self.now - 60)
        yield self.assert_trigger_metric('movingAverage(' + metric + ',3)', 10, state.OK)
        yield self.db.sendMetric(metric, metric, self.now, 40)
        yield self.trigger.check(now=self.now)
        yield self.assert_trigger_metric('movingAverage(' + metric + ',3)', 20, state.WARN)

    @trigger('test-mem-free')
    @inlineCallbacks
    def testMemoryCalculation(self):
        metric1 = 'MoiraFuncTest.system.vm-graphite2.memory.Cached'
        metric2 = 'MoiraFuncTest.system.vm-graphite2.memory.MemFree'
        metric3 = 'MoiraFuncTest.system.vm-graphite2.memory.MemTotal'
        yield self.sendTrigger('{"name": "test trigger", "targets": ["scale(divideSeries(sum(movingAverage(' + metric2 +
                               ',3),movingAverage(' + metric1 + ',3)),movingAverage(' + metric3 +
                               ',3)),100)"],  "warn_value": 60, "error_value": 90, "ttl":"600" }')
        yield self.db.sendMetric(metric1, metric1, self.now - 1, 1000)
        yield self.db.sendMetric(metric2, metric2, self.now - 1, 1000)
        yield self.db.sendMetric(metric3, metric3, self.now - 1, 4000)
        yield self.trigger.check()
        yield self.assert_trigger_metric(1, 50, state.OK)

    @trigger('test-events')
    @inlineCallbacks
    def testEventGeneration(self):
        metric = 'MoiraFuncTest.metric.one'
        yield self.sendTrigger('{"name": "test trigger", "targets": ["' + metric +
                               '"],  "warn_value": 60, "error_value": 90, "ttl":"600" }')
        yield self.db.sendMetric(metric, metric, self.now - 180, 1000)
        yield self.db.sendMetric(metric, metric, self.now - 60, 1000)
        yield self.trigger.check()
        yield self.db.sendMetric(metric, metric, self.now, 10)
        yield self.protocol.messageReceived(None, "moira-func-test", '{"pattern":"' + metric +
                                            '", "metric":"' + metric + '"}', nocache=True)
        yield self.trigger.check(now=self.now + 1)
        events, total = yield self.db.getEvents()
        self.assertEquals(total, 2)
        self.assertEquals(events[0]["state"], state.OK)
        self.assertEquals(events[1]["state"], state.ERROR)

    @trigger('test-events2')
    @inlineCallbacks
    def testEventGeneration2(self):
        metric = 'MoiraFuncTest.metric.one'
        yield self.sendTrigger('{"name": "test trigger", "targets": ["' + metric +
                               '"],  "warn_value": 60, "error_value": 90, "ttl":"600" }')
        yield self.db.sendMetric(metric, metric, self.now - 180, 1000)
        yield self.db.sendMetric(metric, metric, self.now - 60, 1000)
        yield self.trigger.check()
        yield self.trigger.check()
        yield self.trigger.check()
        events, total = yield self.db.getEvents()
        self.assertEquals(total, 1)
        self.assertEquals(events[0]["state"], state.ERROR)

    @trigger('test-events3')
    @inlineCallbacks
    def testEventGeneration3(self):
        metric = 'MoiraFuncTest.metric.one'
        yield self.sendTrigger('{"name": "test trigger", "targets": ["' + metric +
                               '"],  "warn_value": 1, "error_value": 5, "ttl":"600", "ttl_state": "OK" }')
        yield self.db.sendMetric(metric, metric, self.now - 1, 1)
        yield self.trigger.check()
        self.assert_trigger_metric(metric, 1, state.WARN)
        yield self.trigger.check(now=self.now + 120)
        self.assert_trigger_metric(metric, 1, state.WARN)
        yield self.trigger.check(now=self.now + 601)
        yield self.trigger.check(now=self.now + 602)
        self.assert_trigger_metric(metric, None, state.OK)
        events, total = yield self.db.getEvents()
        self.assertEquals(total, 2)
        self.assertEquals(events[0]["state"], state.OK)
        self.assertEquals(events[0]["metric"], metric)
        self.assertNotIn("value", events[0])
        self.assertEquals(events[1]["state"], state.WARN)
        self.assertEquals(events[1]["metric"], metric)
        self.assertEquals(events[1]["value"], 1)

    @trigger('test-event-schedule')
    @inlineCallbacks
    def testEventSchedule(self):
        metric = 'MoiraFuncTest.metric.one'
        yield self.sendTrigger('{"name": "test trigger", "targets": ["' + metric +
                               '"],  "warn_value": 1, "error_value": 5, "ttl":"600", "ttl_state": "OK", \
                               "sched":{"days":[ \
                               {"enabled":false,"name":"Mon"}, \
                               {"enabled":true,"name":"Tue"}, \
                               {"enabled":true,"name":"Wed"}, \
                               {"enabled":true,"name":"Thu"}, \
                               {"enabled":true,"name":"Fri"}, \
                               {"enabled":true,"name":"Sat"}, \
                               {"enabled":false,"name":"Sun"}], \
                               "startOffset":0, \
                               "endOffset":1439, \
                               "tzOffset":0}}')

        # generate event
        yield self.db.sendMetric(metric, metric, 1444471200, 1)  # Saturday @ 10:00am (UTC)
        yield self.trigger.check(now=1444471200)
        self.assert_trigger_metric(metric, 1, state.WARN)

        # don't generate event
        yield self.db.sendMetric(metric, metric, 1444644000, 10)  # Monday @ 10:00am (UTC)
        yield self.trigger.check(now=1444644000)
        self.assert_trigger_metric(metric, 10, state.ERROR)

        events, total = yield self.db.getEvents()
        self.assertEquals(total, 1)

        # generate missed event
        yield self.db.sendMetric(metric, metric, 1444730400, 10)  # Tuesday @ 10:00am (UTC)
        yield self.trigger.check(now=1444730400)
        self.assert_trigger_metric(metric, 10, state.ERROR)

        # check event not duplicated
        yield self.db.sendMetric(metric, metric, 1444730460, 11)  # Tuesday @ 10:01am (UTC)
        yield self.trigger.check(now=1444730460)
        self.assert_trigger_metric(metric, 11, state.ERROR)

        events, total = yield self.db.getEvents()
        self.assertEquals(total, 2)

    @trigger('test-ttl')
    @inlineCallbacks
    def testTTLExpiration(self):
        metric = 'MoiraFuncTest.metric.one'
        yield self.sendTrigger('{"name": "test trigger", "targets": ["' +
                               metric + '"], "warn_value": 60, "error_value": 90, "ttl":60 }')
        yield self.db.sendMetric(metric, metric, self.now - 180, 1000)
        yield self.trigger.check()
        yield self.assert_trigger_metric(metric, 1000, state.ERROR)
        yield self.trigger.check()
        yield self.assert_trigger_metric(metric, None, state.NODATA)
        yield self.db.sendMetric(metric, metric, self.now, 10)
        yield self.trigger.check(now=self.now + 60)
        yield self.assert_trigger_metric(metric, 10, state.OK)
        yield self.trigger.check(now=self.now + 120)
        events, total = yield self.db.getEvents()
        self.assertEquals(total, 3)
        self.assertEquals(events[0]["state"], state.OK)
        self.assertEquals(events[1]["state"], state.NODATA)
        self.assertEquals(events[2]["state"], state.ERROR)

    @trigger('test-ttl')
    @inlineCallbacks
    def testTTLExpiration2(self):
        pattern = 'MoiraFuncTest.metric.*'
        metric1 = 'MoiraFuncTest.metric.one'
        metric2 = 'MoiraFuncTest.metric.two'
        yield self.sendTrigger('{"name": "test trigger", "targets": ["sumSeries(' + pattern + ')"], \
                               "warn_value": 60, "error_value": 90, "ttl":120 }')
        yield self.db.sendMetric(pattern, metric1, self.now - 3600, 5)
        yield self.db.sendMetric(pattern, metric2, self.now - 60, 5)
        yield self.trigger.check()
        yield self.assert_trigger_metric('sumSeries(' + pattern + ')', 5, state.OK)
        yield self.db.cleanupMetricValues(metric2, self.now)
        yield self.trigger.check(now=self.now + 61)
        yield self.assert_trigger_metric('sumSeries(' + pattern + ')', 5, state.OK)
        yield self.trigger.check(now=self.now + 62)
        yield self.assert_trigger_metric('sumSeries(' + pattern + ')', None, state.NODATA)

    @trigger('test-ttl')
    @inlineCallbacks
    def testTTLExpiration3(self):
        metric = 'MoiraFuncTest.metric.one'
        yield self.sendTrigger('{"name": "test trigger", "targets": ["' +
                               metric + '"], "warn_value": 1, "error_value": 5, "ttl":600, "ttl_state":"OK" }')
        yield self.db.sendMetric(metric, metric, self.now - 2400, 1)
        yield self.trigger.check(now=self.now - 2400)
        yield self.assert_trigger_metric(metric, 1, state.WARN)
        yield self.trigger.check(now=self.now - 2200)
        yield self.trigger.check(now=self.now - 1000)
        yield self.trigger.check(now=self.now)
        yield self.assert_trigger_metric(metric, None, state.OK)
        yield self.db.sendMetric(metric, metric, self.now, 1)
        yield self.trigger.check(now=self.now)
        yield self.assert_trigger_metric(metric, 1, state.WARN)
        yield self.trigger.check(now=self.now + 1)

    @trigger('test-ttl')
    @inlineCallbacks
    def testTTLExpiration4(self):
        metric = 'MoiraFuncTest.metric.one'
        yield self.sendTrigger('{"name": "test trigger", "targets": ["' +
                               metric + '"], "warn_value": 60, "error_value": 90, "ttl":60 }')
        yield self.db.sendMetric(metric, metric, self.now - 180, 1000)
        yield self.trigger.check()
        yield self.assert_trigger_metric(metric, 1000, state.ERROR)
        yield self.db.sendMetric(metric, metric, self.now - 120, 1000)
        yield self.trigger.check()
        yield self.assert_trigger_metric(metric, None, state.NODATA)

    @trigger('test-data-delay')
    @inlineCallbacks
    def testDataDelay(self):
        metric = 'MoiraFuncTest.metric.one'
        yield self.sendTrigger('{"name": "test trigger", "targets": ["' +
                               metric + '"], "warn_value": 60, "error_value": 90, "ttl":600 }')
        yield self.db.sendMetric(metric, metric, self.now, 10)
        yield self.trigger.check()
        yield self.assert_trigger_metric(metric, 10, state.OK)
        yield self.trigger.check(now=self.now + 60)
        yield self.db.sendMetric(metric, metric, self.now + 1200, 20)
        yield self.trigger.check(now=self.now + 1200)
        yield self.assert_trigger_metric(metric, 20, state.OK)
        events, total = yield self.db.getEvents()
        self.assertEquals(total, 1)
        self.assertEquals(events[0]["state"], state.OK)

    @trigger('test-nodata-remind')
    @inlineCallbacks
    def testNodataRemind(self):
        metric = 'MoiraFuncTest.metric.one'
        yield self.sendTrigger('{"name": "test trigger", "targets": ["' +
                               metric + '"], "warn_value": 60, "error_value": 90, "ttl":600 }')
        yield self.db.sendMetric(metric, metric, self.now - 1000, 10)
        yield self.trigger.check(now=self.now - 60)
        yield self.trigger.check()
        yield self.assert_trigger_metric(metric, None, state.NODATA)
        yield self.trigger.check(now=self.now + 86400)
        yield self.trigger.check(now=self.now + 86460)
        events, total = yield self.db.getEvents()
        self.assertEquals(total, 3)
        self.assertEquals(events[0]["state"], state.NODATA)
        self.assertEquals(events[0]["old_state"], state.NODATA)

    @trigger('test-error-noremind')
    @inlineCallbacks
    def testErrorNoRemind(self):
        metric = 'MoiraFuncTest.metric.one'
        yield self.sendTrigger('{"name": "test trigger", "targets": ["' +
                               metric + '"], "warn_value": 60, "error_value": 90, "ttl":600, "ttl_state": "OK" }')
        yield self.db.sendMetric(metric, metric, self.now, 0)
        yield self.trigger.check()
        yield self.assert_trigger_metric(metric, 0, state.OK)
        yield self.trigger.check(now=self.now + 660)
        yield self.trigger.check(now=self.now + 660)
        yield self.assert_trigger_metric(metric, None, state.OK)
        yield self.trigger.check(now=self.now + 88460)
        yield self.trigger.check(now=self.now + 88460)
        yield self.db.sendMetric(metric, metric, self.now + 88520, 100)
        yield self.db.sendMetric(metric, metric, self.now + 88580, 100)
        yield self.trigger.check(now=self.now + 88580)
        events, total = yield self.db.getEvents()
        self.assertEquals(total, 2)
        self.assertEquals(events[0]["state"], state.ERROR)

    @trigger('test-nodata-deletion')
    @inlineCallbacks
    def testNodataDeletion(self):
        metric = 'MoiraFuncTest.metric.one'
        yield self.sendTrigger('{"name": "test trigger", "targets": ["' +
                               metric + '"], "warn_value": 60, "error_value": 90, "ttl":600, "ttl_state": "DEL" }')
        yield self.db.sendMetric(metric, metric, self.now - 1000, 0)
        yield self.trigger.check(now=self.now - 60)
        yield self.trigger.check()
        check = yield self.db.getTriggerLastCheck(self.trigger.id)
        self.assertIs(check["metrics"].get(metric), None)

    @trigger('test-map-reduce')
    @inlineCallbacks
    def testMapReduce(self):
        pattern = 'MoiraFuncTest.*.metric.{free,total}'
        metric1 = 'MoiraFuncTest.one.metric.free'
        metric2 = 'MoiraFuncTest.one.metric.total'
        metric3 = 'MoiraFuncTest.two.metric.free'
        metric4 = 'MoiraFuncTest.two.metric.total'
        yield self.sendTrigger('{"name": "test trigger", "targets": \
                               ["aliasByNode(reduceSeries(mapSeries(' + pattern + ',1), \
                               \\"asPercent\\",3,\\"free\\",\\"total\\"),1)"], \
                               "warn_value": 60, "error_value": 90 }')
        yield self.db.sendMetric(pattern, metric1, self.now - 1, 60)
        yield self.db.sendMetric(pattern, metric2, self.now - 1, 100)
        yield self.db.sendMetric(pattern, metric3, self.now - 1, 30)
        yield self.db.sendMetric(pattern, metric4, self.now - 1, 60)
        yield self.trigger.check()
        yield self.assert_trigger_metric('one', 60, state.WARN)
        yield self.assert_trigger_metric('two', 50, state.OK)

    @trigger('test-trigger-cleanup')
    @inlineCallbacks
    def testMetricsCleanup(self):
        metric = 'MoiraFuncTest.metric.one'
        yield self.sendTrigger('{"name": "test trigger", "targets": ["' +
                               metric + '"], "warn_value": 60, "error_value": 90 }')
        yield self.db.sendMetric(metric, metric, self.now - 3600, 1)
        yield self.db.sendMetric(metric, metric, self.now - 60, 1)
        yield self.trigger.check(now=self.now + 60, cache_ttl=0)
        yield self.assert_trigger_metric(metric, 1, state.OK)
        values = yield self.db.getMetricsValues([metric], self.now - 3600)
        self.assertEquals(len(values), 1)
        self.assertEquals(len(values[0]), 1)
        yield self.deleteTrigger()
        yield self.db.sendMetric(metric, metric, self.now, 1)
        yield self.protocol.messageReceived(None, "moira-func-test", '{"pattern":"' + metric +
                                            '", "metric":"' + metric + '"}')
        values = yield self.db.getMetricsValues([metric], self.now - 3600)
        self.assertEquals(len(values), 1)
        self.assertEquals(len(values[0]), 0)

    @trigger('test-schedule')
    @inlineCallbacks
    def testTriggerSchedule(self):
        metric = 'MoiraFuncTest.metric.one'
        yield self.sendTrigger('{"name": "test trigger", "targets": ["' +
                               metric + '"], "warn_value": 60, "error_value": 90, \
                               "sched":{"days":[ \
                               {"enabled":true,"name":"Mon"}, \
                               {"enabled":true,"name":"Tue"}, \
                               {"enabled":true,"name":"Wed"}, \
                               {"enabled":true,"name":"Thu"}, \
                               {"enabled":true,"name":"Fri"}, \
                               {"enabled":true,"name":"Sat"}, \
                               {"enabled":true,"name":"Sun"}], \
                               "startOffset":480, \
                               "endOffset":1199, \
                               "tzOffset":-300}}')

        yield self.trigger.init(now=self.now)
        day_begin = self.now - self.now % (3600 * 24)
        self.assertFalse(self.trigger.isSchedAllows(day_begin + 3 * 3600 - 1))
        self.assertTrue(self.trigger.isSchedAllows(day_begin + 3 * 3600))
        self.assertTrue(self.trigger.isSchedAllows(day_begin + 15 * 3600 - 1))
        self.assertFalse(self.trigger.isSchedAllows(day_begin + 15 * 3600))

    @trigger('test-sum-with-null')
    @inlineCallbacks
    def testSumWithNull(self):
        yield self.sendTrigger('{"name": "test trigger", "targets": [" \
                               sumSeries(metric.one, metric.two)"], "warn_value": 60, "error_value": 90}')

        yield self.db.addPatternMetric("metric.two", "metric.two")
        yield self.db.sendMetric("metric.one", "metric.one", self.now, 1)
        yield self.trigger.check(now=self.now + 60, cache_ttl=0)
        yield self.assert_trigger_metric("sumSeries(metric.one,metric.two)", 1, state.OK)

    @trigger('test-sum-with-null2')
    @inlineCallbacks
    def testSumWithNull2(self):
        yield self.sendTrigger('{"name": "test trigger", "targets": [" \
                               sumSeries(metric.*)"], "warn_value": 60, "error_value": 90}')

        yield self.db.addPatternMetric("metric.*", "metric.one")
        yield self.db.addPatternMetric("metric.*", "metric.two")
        yield self.db.sendMetric("metric.one", "metric.one", self.now, 1)
        yield self.trigger.check(now=self.now + 60, cache_ttl=0)
        yield self.assert_trigger_metric("sumSeries(metric.*)", 1, state.OK)

    @trigger('test-var-metrics')
    @inlineCallbacks
    def testVariableMetrics(self):
        yield self.db.addPatternMetric("metric.*", "metric.one")
        yield self.db.addPatternMetric("metric.*", "metric.two")
        yield self.sendTrigger('{"name": "test trigger", "targets": [" \
                               maximumAbove(metric.*, 0)"], "warn_value": 60, "error_value": 90}')
        yield self.db.sendMetric("metric.one", "metric.one", self.now, 1)
        yield self.trigger.check(now=self.now + 60, cache_ttl=0)
        yield self.db.cleanupMetricValues("metric.one", self.now + 3600)
        yield self.db.sendMetric("metric.two", "metric.two", self.now + 60, 1)
        yield self.trigger.check(now=self.now + 120, cache_ttl=0)
        yield self.assert_trigger_metric("metric.one", 1, state.OK)

    @trigger('test-schedule2')
    @inlineCallbacks
    def testTriggerSchedule2(self):
        metric = 'MoiraFuncTest.metric.one'
        yield self.sendTrigger('{"name": "test trigger", "targets": ["' +
                               metric + '"], "warn_value": 60, "error_value": 90, \
                               "sched":{"days":[ \
                               {"enabled":true,"name":"Mon"}, \
                               {"enabled":true,"name":"Tue"}, \
                               {"enabled":true,"name":"Wed"}, \
                               {"enabled":true,"name":"Thu"}, \
                               {"enabled":true,"name":"Fri"}, \
                               {"enabled":true,"name":"Sat"}, \
                               {"enabled":true,"name":"Sun"}], \
                               "startOffset":0, \
                               "endOffset":1439, \
                               "tzOffset": -300}}')

        yield self.trigger.init(now=self.now)
        day_begin = self.now - self.now % (3600 * 24)
        for h in range(0, 24):
            self.assertTrue(self.trigger.isSchedAllows(day_begin + 3600 * h))

    @trigger('test-trigger-score')
    @inlineCallbacks
    def testTriggerScore(self):
        yield self.sendTrigger('{"name": "test trigger", "targets": [" \
                               metric"], "warn_value": 1, "error_value": 2}')

        yield self.db.addPatternMetric("metric", "metric")
        yield self.db.sendMetric("metric", "metric", self.now, 0)
        yield self.trigger.check(now=self.now, cache_ttl=0)
        check = yield self.db.getTriggerLastCheck(self.trigger.id)
        self.assertEquals(0, check["score"])

        yield self.db.sendMetric("metric", "metric", self.now + 60, 1)
        yield self.trigger.check(now=self.now + 60, cache_ttl=0)
        check = yield self.db.getTriggerLastCheck(self.trigger.id)
        self.assertEquals(1, check["score"])

        yield self.db.sendMetric("metric", "metric", self.now + 120, 2)
        yield self.trigger.check(now=self.now + 120, cache_ttl=0)
        check = yield self.db.getTriggerLastCheck(self.trigger.id)
        self.assertEquals(100, check["score"])

    @trigger('test-late-metric')
    @inlineCallbacks
    def testLateMetrics(self):
        yield self.sendTrigger('{"name": "test trigger", "targets": [" \
                               metric"], "warn_value": 1, "error_value": 2}')

        yield self.db.addPatternMetric("metric", "metric")
        yield self.db.sendMetric("metric", "metric", self.now, 0)
        yield self.trigger.check(now=self.now, cache_ttl=0)
        yield self.assert_trigger_metric("metric", 0, state.OK)

        yield self.db.sendMetric("metric", "metric", self.now - 60, 2)
        yield self.trigger.check(now=self.now + 60, cache_ttl=0)
        yield self.assert_trigger_metric("metric", 0, state.OK)

        yield self.db.sendMetric("metric", "metric", self.now + 120, 0)
        yield self.trigger.check(now=self.now + 120, cache_ttl=0)

        yield self.db.sendMetric("metric", "metric", self.now + 60, 1)
        yield self.trigger.check(now=self.now + 180, cache_ttl=0)
        yield self.assert_trigger_metric("metric", 0, state.OK)

        events, total = yield self.db.getEvents()
        self.assertEquals(total, 3)
        self.assertEquals(events[1]["state"], state.WARN)

    @trigger('test-simple-trigger-realtime-behaviour')
    @inlineCallbacks
    def testSimpleTriggerRealtimeBehaviour(self):
        metric = 'MoiraFuncTest.metric.one'
        yield self.sendTrigger('{"name": "test trigger", "targets": ["' +
                               metric + '"], "warn_value": 60, "error_value": 90, "ttl":600 }')

        yield self.db.sendMetric(metric, metric, self.now - 60, 10)
        yield self.trigger.check(now=self.now)
        yield self.assert_trigger_metric(metric, 10, state.OK)

        yield self.db.sendMetric(metric, metric, self.now, 100)
        yield self.trigger.check(now=self.now)
        yield self.assert_trigger_metric(metric, 100, state.ERROR)

    @trigger('test-complex-trigger-conservative-behaviour')
    @inlineCallbacks
    def testComplexTriggerConservativeBehaviour(self):
        metric = 'MoiraFuncTest.metric.one'
        pattern = 'MoiraFuncTest.metric.*'
        yield self.sendTrigger('{"name": "test trigger", "targets": ["' +
                               pattern + '"], "warn_value": 60, "error_value": 90, "ttl":600 }')

        yield self.db.sendMetric(pattern, metric, self.now - 60, 10)
        yield self.trigger.check(now=self.now)
        yield self.assert_trigger_metric(metric, 10, state.OK)

        yield self.db.sendMetric(pattern, metric, self.now, 100)
        yield self.trigger.check(now=self.now)
        yield self.assert_trigger_metric(metric, 10, state.OK)

        yield self.trigger.check(now=self.now + 60)
        yield self.assert_trigger_metric(metric, 100, state.ERROR)

    @trigger('test-trigger-on-multiple-metrics-does-not-produce-preliminary-real-time-alert')
    @inlineCallbacks
    def testComplexTriggerMultipleMetricsNoPreliminaryAlert(self):
        metric1 = 'metric.one'
        metric2 = 'metric.two'
        pattern = 'metric.*'
        target = 'maxSeries(movingAverage(%s, 3, \\"min\\"))' % pattern
        targetMetric = 'maxSeries(movingAverage(metric.one,3),movingAverage(metric.two,3))'
        yield self.sendTrigger(
            '{"name": "tt", "targets": ["%s"], "warn_value": 99, "error_value": 100, "ttl":"600" }' % target)

        yield self.db.sendMetric(pattern, metric1, self.now, 10)
        yield self.db.sendMetric(pattern, metric2, self.now, 20)
        yield self.db.sendMetric(pattern, metric1, self.now + 60, 110)
        yield self.db.sendMetric(pattern, metric2, self.now + 60, 120)
        yield self.db.sendMetric(pattern, metric1, self.now + 120, 210)
        yield self.db.sendMetric(pattern, metric2, self.now + 120, 220)

        yield self.trigger.check(now=self.now + 180)
        yield self.assert_trigger_metric(targetMetric, 20, state.OK)

        yield self.db.sendMetric(pattern, metric1, self.now + 180, 310)

        yield self.trigger.check(now=self.now + 180)
        yield self.assert_trigger_metric(targetMetric, 20, state.OK)

        yield self.db.sendMetric(pattern, metric2, self.now + 180, 320)

        yield self.trigger.check(now=self.now + 180)
        yield self.assert_trigger_metric(targetMetric, 20, state.OK)

        yield self.trigger.check(now=self.now + 240 - 1)
        yield self.assert_trigger_metric(targetMetric, 20, state.OK)

        yield self.trigger.check(now=self.now + 240)
        yield self.assert_trigger_metric(targetMetric, 120., state.ERROR)

    @trigger('test-moving-average-bootstrap-with-no-realtime-alerting')
    @inlineCallbacks
    def testMovingAverageBootstrapWithNoRealTimeAlerting(self):
        yield self.movingAverageBootstrap(allowRealTimeAlerting=False)

    @trigger('test-moving-average-bootstrap-with-realtime-alerting')
    @inlineCallbacks
    def testMovingAverageBootstrapWithRealTimeAlerting(self):
        yield self.movingAverageBootstrap(allowRealTimeAlerting=True)

    @inlineCallbacks
    def movingAverageBootstrap(self, allowRealTimeAlerting):
        yield self.sendTrigger(
            '{"name": "t", "targets": ["movingAverage(m, 2)"],  "warn_value": 1, "error_value": 90, "ttl":"600" }')
        for n in range(0, 10):
            yield self.db.sendMetric('m', 'm', self.now - 60 * (10 - n), n)
        yield self.trigger.check(fromTime=self.now - 300, now=self.now)
        fromTime = str(self.now - 180)
        endTime = str(self.now - 60)
        rc = datalib.createRequestContext(fromTime, endTime, allowRealTimeAlerting)
        result = yield self.trigger.get_timeseries(rc)
        ts = result[1][0]
        self.assertEqual(ts[0], 6.5)
