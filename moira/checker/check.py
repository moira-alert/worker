import sys
import os

sys.path.append(
    os.path.abspath(
        os.path.join(
            os.path.abspath(
                os.path.dirname(__file__)),
            '../../')))


import random
from datetime import datetime, timedelta
from time import time
from twisted.python import log
from twisted.python.logfile import DailyLogFile
from twisted.internet import defer, reactor, task
from moira.graphite.evaluator import evaluateTarget
from moira.graphite import datalib
from moira.metrics import spy, graphite
from moira.db import Db
from moira import config
from moira.checker import state
from moira.checker import expression


PERFORM_INTERVAL = 0.01
ERROR_TIMEOUT = 10


class TriggersCheck():

    def __init__(self, db):
        self.db = db

    def start(self):
        self.t = task.LoopingCall(self.perform)
        self.finished = self.t.start(PERFORM_INTERVAL, now=False)
        log.msg("Checker service started")

    @defer.inlineCallbacks
    def stop(self):
        self.t.stop()
        yield self.finished

    @defer.inlineCallbacks
    def perform(self):
        try:
            trigger_id = yield self.db.getTriggerToCheck()
            while trigger_id is not None:
                start = reactor.seconds()
                yield TriggersCheck.check(self.db, trigger_id)
                end = reactor.seconds()
                spy.TRIGGER_CHECK.report(end - start)
                trigger_id = yield self.db.getTriggerToCheck()
            yield task.deferLater(reactor, random.uniform(PERFORM_INTERVAL * 10, PERFORM_INTERVAL * 20), lambda: None)
        except GeneratorExit:
            pass
        except:
            spy.TRIGGER_CHECK_ERRORS.report(0)
            log.err()
            yield task.deferLater(reactor, ERROR_TIMEOUT, lambda: None)

    @staticmethod
    def isTriggerSchedEnabled(trigger, now):
        sched = trigger.get('sched')
        if sched is None:
            return True

        timestamp = now - now % 60 - sched["tzOffset"] * 60
        date = datetime.fromtimestamp(timestamp)
        if not sched['days'][date.weekday()]['enabled']:
            return False
        day_start = datetime.fromtimestamp(timestamp - timestamp % (24 * 3600))
        start_datetime = day_start + timedelta(minutes=sched["startOffset"])
        end_datetime = day_start + timedelta(minutes=sched["endOffset"])
        if date < start_datetime:
            return False
        if date > end_datetime:
            return False
        return True

    @staticmethod
    @defer.inlineCallbacks
    def check(db, trigger_id, fromTime=None, now=None, cache_ttl=60):
        trigger_json, trigger = yield db.getTrigger(trigger_id, tags=True)
        log.msg("Checking trigger %s" % trigger_id)
        if trigger_json is None:
            raise StopIteration
        maintenance = False
        for tag in trigger["tags"]:
            tag_data = yield db.getTag(tag)
            if tag_data.get('maintenance'):
                maintenance = True
                break
        last_check = yield db.getTriggerLastCheck(trigger_id)
        if now is None:
            now = int(time())
        if last_check is None:
            last_check = {"metrics": {}, "state": state.NODATA, "timestamp": (fromTime or now) - 600}
        if fromTime is None:
            fromTime = last_check.get("timestamp", now)
        requestContext = datalib.createRequestContext(str(fromTime - 600), str(now))
        check = {"metrics": {}, "state": state.OK, "timestamp": now}
        ttl = trigger.get("ttl")

        try:
            targets = trigger.get("targets", [trigger.get("target")])
            target_time_series = {}
            target_number = 0
            max_step = 0
            for target in targets:
                target_number += 1
                time_series = yield evaluateTarget(requestContext, target)
                target_time_series["t%s" % target_number] = time_series

                if len(time_series) > 0:
                    max_step = max(max_step, max(map(lambda ts: ts.step, time_series)))
                for metric in requestContext['metrics']:
                    yield db.cleanupMetricValues(metric, now - config.METRICS_TTL,
                                                 cache_key=metric, cache_ttl=cache_ttl)

            for time_serie in target_time_series["t1"]:

                last_check["metrics"][time_serie.name] = last_metric_state = last_check["metrics"].get(
                    time_serie.name, {
                        "state": state.NODATA,
                        "timestamp": time_serie.start})

                check["metrics"][time_serie.name] = metric_state = last_metric_state.copy()

                for value_timestamp in xrange(time_serie.start, now + time_serie.step, time_serie.step):

                    if value_timestamp <= last_metric_state["timestamp"]:
                        continue

                    expression_values = {}

                    for target_number in xrange(1, len(targets) + 1):
                        target_name = "t%s" % target_number
                        tts = time_serie
                        if target_number > 1:
                            if len(target_time_series[target_name]) > 1:
                                raise "Target #%s has more than one timeseries" % target_number
                            if len(target_time_series[target_name]) == 0:
                                raise "Target #%s has no timeseries" % target_number
                            tts = target_time_series[target_name][0]

                        value_index = (value_timestamp - tts.start) / tts.step
                        time_serie_value = tts[value_index] if len(tts) > value_index else None
                        expression_values[target_name] = time_serie_value

                    first_target_value = expression_values["t1"]

                    if None in expression_values.values():
                        continue

                    expression_values.update({'warn_value': trigger.get('warn_value'),
                                              'error_value': trigger.get('error_value')})

                    if ttl and value_timestamp + ttl < last_check["timestamp"]:
                        log.msg("Metric %s TTL expired for value %s with timestamp %s" %
                                (time_serie.name, first_target_value, value_timestamp))
                        metric_state["state"] = trigger.get("ttl_state", state.NODATA)
                        metric_state["timestamp"] = value_timestamp + ttl
                        if "value" in metric_state:
                            del metric_state["value"]
                        yield TriggersCheck.compare_state(db, trigger, metric_state,
                                                          last_metric_state,
                                                          value_timestamp + ttl, value=None,
                                                          metric=time_serie.name,
                                                          maintenance=maintenance)
                    else:
                        metric_state["state"] = expression.getExpression(trigger.get('expression'),
                                                                         **expression_values)
                        metric_state["value"] = first_target_value
                        metric_state["timestamp"] = value_timestamp
                        yield TriggersCheck.compare_state(db, trigger, metric_state,
                                                          last_metric_state,
                                                          value_timestamp, value=first_target_value,
                                                          metric=time_serie.name,
                                                          maintenance=maintenance)

            if ttl and len(check["metrics"]) == 0:
                check["state"] = trigger.get("ttl_state", state.NODATA)
                check["msg"] = "Trigger has no metrics"
                yield TriggersCheck.compare_state(db, trigger, check, last_check, now, maintenance=maintenance)

            if ttl:
                for metric, metric_state in check["metrics"].iteritems():
                    if metric_state["timestamp"] + ttl < last_check["timestamp"]:
                        log.msg("Metric %s TTL expired for state %s" % (metric, metric_state))
                        metric_state["state"] = trigger.get("ttl_state", state.NODATA)
                        metric_state["timestamp"] += ttl
                        if "value" in metric_state:
                            del metric_state["value"]
                        yield TriggersCheck.compare_state(db, trigger, metric_state,
                                                          last_check["metrics"][metric],
                                                          metric_state["timestamp"],
                                                          metric=metric,
                                                          maintenance=maintenance)

        except StopIteration:
            raise
        except:
            log.err()
            check["state"] = state.EXCEPTION
            check["msg"] = "Trigger evaluation exception"
            yield TriggersCheck.compare_state(db, trigger, check, last_check, now)
        yield db.setTriggerLastCheck(trigger_id, check)

    @staticmethod
    @defer.inlineCallbacks
    def compare_state(
            db,
            trigger,
            current_state,
            last_state,
            timestamp,
            value=None,
            metric=None,
            maintenance=False):
        current_state_value = current_state["state"]
        last_state_value = last_state["state"]
        if current_state_value != last_state_value or \
                (last_state.get("suppressed") and current_state_value != state.OK):
            event = {
                "trigger_id": trigger["id"],
                "state": current_state_value,
                "old_state": last_state_value,
                "timestamp": timestamp,
                "metric": metric
            }
            current_state["event_timestamp"] = timestamp
            if value is not None:
                event["value"] = value
            if TriggersCheck.isTriggerSchedEnabled(trigger, timestamp):
                if not maintenance:
                    log.msg("Writing new event: %s" % event)
                    yield db.pushEvent(event)
                    current_state["suppressed"] = False
                else:
                    current_state["suppressed"] = True
                    log.msg("Event %s suppressed due maintenance" % str(event))
            else:
                current_state["suppressed"] = True
                log.msg("Event %s suppressed due trigger schedule" % str(event))
        last_state["state"] = current_state_value


def run(callback):

    db = Db()
    datalib.db = db
    init = db.startService()
    init.addCallback(callback)

    reactor.run()


def main(number):

    def get_metrics():
        return [
            ("checker.time.%s.%s" %
             (config.HOSTNAME,
              number),
                spy.TRIGGER_CHECK.get_metrics()["sum"]),
            ("checker.triggers.%s.%s" %
             (config.HOSTNAME,
              number),
                spy.TRIGGER_CHECK.get_metrics()["count"]),
            ("checker.errors.%s.%s" %
             (config.HOSTNAME,
              number),
                spy.TRIGGER_CHECK_ERRORS.get_metrics()["count"])]

    graphite.sending(get_metrics)

    def start(db):
        checker = TriggersCheck(db)
        checker.start()
        reactor.addSystemEventTrigger('before', 'shutdown', checker.stop)

    run(start)


def check(trigger_id):

    @defer.inlineCallbacks
    def start(db):
        yield TriggersCheck.check(db, trigger_id)
        reactor.stop()

    run(start)


if __name__ == '__main__':

    parser = config.get_parser()
    parser.add_argument('-t', help='check single trigger by id and exit')
    parser.add_argument('-n', help='checker number', type=int)
    args = parser.parse_args()

    config.CONFIG_PATH = args.c
    config.LOG_DIRECTORY = args.l

    config.read()

    if args.l != "stdout":
        logfile = DailyLogFile(
            "checker-{0}.log".format(args.n),
            os.path.abspath(
                config.LOG_DIRECTORY))
        log.startLogging(logfile)
    else:
        log.startLogging(sys.stdout)

    if args.t:
        check(args.t)
    else:
        main(args.n)
