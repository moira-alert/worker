from twisted.python import log
from twisted.internet import defer
from datetime import datetime, timedelta
from time import time
from moira.graphite.evaluator import evaluateTarget
from moira.graphite import datalib
from moira.checker import state
from moira.checker import expression
from moira import config


class TargetTimeSeries(dict):

    def __init__(self, *arg, **kwargs):
        super(TargetTimeSeries, self).__init__(*arg, **kwargs)
        self.other_targets_names = {}

    def get_expression_values(self, t1, timestamp):
        expression_values = {}
        for target_number in xrange(1, len(self) + 1):
            target_name = "t%s" % target_number
            tN = self[target_number][0] if target_number > 1 else t1
            value_index = (timestamp - tN.start) / tN.step
            tN_value = tN[value_index] if len(tN) > value_index else None
            expression_values[target_name] = tN_value
            if tN_value is None:
                break
        return expression_values

    def set_state_value(self, metric_state, expression_values, tN):
        if expression_values is None:
            if "value" in metric_state:
                del metric_state["value"]
        else:
            metric_state["value"] = expression_values[tN]

    def update_state(self, t1, check, expression_state, expression_values, timestamp):
        metric_state = check["metrics"][t1.name]
        metric_state["state"] = expression_state
        metric_state["timestamp"] = timestamp
        self.set_state_value(metric_state, expression_values, "t1")

        for tN, tName in self.other_targets_names.iteritems():
            other_metric_state = check["metrics"][tName]
            other_metric_state["state"] = expression_state
            other_metric_state["timestamp"] = timestamp
            self.set_state_value(other_metric_state, expression_values, tN)


class Trigger:

    def __init__(self, id, db):
        self.id = id
        self.db = db

    @defer.inlineCallbacks
    def init(self, now, fromTime=None):
        self.maintenance = 0
        json, self.struct = yield self.db.getTrigger(self.id)
        if json is None:
            defer.returnValue(False)
        for tag in self.struct["tags"]:
            tag_data = yield self.db.getTag(tag)
            maintenance = tag_data.get('maintenance', 0)
            if maintenance > self.maintenance:
                self.maintenance = maintenance
                break
        self.ttl = self.struct.get("ttl")
        self.ttl_state = self.struct.get("ttl_state", state.NODATA)
        self.last_check = yield self.db.getTriggerLastCheck(self.id)
        if self.last_check is None:
            begin = (fromTime or now) - 3600
            self.last_check = {"metrics": {}, "state": state.NODATA, "timestamp": begin}
        defer.returnValue(True)

    @defer.inlineCallbacks
    def get_timeseries(self, requestContext):
        targets = self.struct.get("targets", [])
        target_time_series = TargetTimeSeries()
        target_number = 1

        for target in targets:
            time_series = yield evaluateTarget(requestContext, target)

            if target_number > 1:
                if len(time_series) > 1:
                    raise Exception("Target #%s has more than one timeseries" % target_number)
                if len(time_series) == 0:
                    raise Exception("Target #%s has no timeseries" % target_number)
                target_time_series.other_targets_names["t%s" % target_number] = time_series[0].name

            for time_serie in time_series:
                time_serie.last_state = self.last_check["metrics"].get(
                                        time_serie.name, {
                                            "state": state.NODATA,
                                            "timestamp": time_serie.start - 3600})
            target_time_series[target_number] = time_series
            target_number += 1

        defer.returnValue(target_time_series)

    @defer.inlineCallbacks
    def check(self, fromTime=None, now=None, cache_ttl=60):

        now = now or int(time())

        log.msg("Checking trigger %s" % self.id)
        initialized = yield self.init(now, fromTime=fromTime)
        if not initialized:
            raise StopIteration

        if fromTime is None:
            fromTime = self.last_check.get("timestamp", now)

        requestContext = datalib.createRequestContext(str(fromTime - (self.ttl or 600)), str(now))

        check = {"metrics": self.last_check["metrics"].copy(), "state": state.OK, "timestamp": now}
        try:
            time_series = yield self.get_timeseries(requestContext)

            for metric in requestContext['metrics']:
                yield self.db.cleanupMetricValues(metric, now - config.METRICS_TTL,
                                                  cache_key=metric, cache_ttl=cache_ttl)

            if len(time_series) == 0:
                if self.ttl:
                    check["state"] = self.ttl_state
                    check["msg"] = "Trigger has no metrics"
                    yield self.compare_state(check, self.last_check, now)
            else:

                for t_series in time_series.values():
                    for tN in t_series:
                        check["metrics"][tN.name] = tN.last_state.copy()

                for t1 in time_series[1]:

                    metric_state = check["metrics"][t1.name]

                    for value_timestamp in xrange(t1.start, now + t1.step, t1.step):

                        if value_timestamp <= t1.last_state["timestamp"]:
                            continue

                        expression_values = time_series.get_expression_values(t1, value_timestamp)

                        t1_value = expression_values["t1"]

                        if None in expression_values.values():
                            continue

                        expression_values.update({'warn_value': self.struct.get('warn_value'),
                                                  'error_value': self.struct.get('error_value'),
                                                  'PREV_STATE': metric_state['state']})

                        expression_state = expression.getExpression(self.struct.get('expression'),
                                                                    **expression_values)

                        time_series.update_state(t1, check, expression_state, expression_values, value_timestamp)

                        yield self.compare_state(metric_state, t1.last_state,
                                                 value_timestamp, value=t1_value,
                                                 metric=t1.name)

                    # compare with last_check timestamp in case if we have not run checker for a long time
                    if self.ttl and metric_state["timestamp"] + self.ttl < self.last_check["timestamp"]:
                        log.msg("Metric %s TTL expired for state %s" % (t1.name, metric_state))
                        if self.ttl_state == state.DEL and metric_state.get("event_timestamp") is not None:
                            log.msg("Remove metric %s" % t1.name)
                            del check["metrics"][t1.name]
                            for tN, tName in time_series.other_targets_names.iteritems():
                                log.msg("Remove metric %s" % tName)
                                del check["metrics"][tName]
                            for pattern in self.struct.get("patterns"):
                                yield self.db.delPatternMetrics(pattern)
                            continue
                        time_series.update_state(t1, check, state.toMetricState(self.ttl_state), None,
                                                 self.last_check["timestamp"] - self.ttl)
                        yield self.compare_state(metric_state, t1.last_state, metric_state["timestamp"], metric=t1.name)

        except StopIteration:
            raise
        except:
            log.err()
            check["state"] = state.EXCEPTION
            check["msg"] = "Trigger evaluation exception"
            yield self.compare_state(check, self.last_check, now)
        yield self.db.setTriggerLastCheck(self.id, check)

    @defer.inlineCallbacks
    def compare_state(self,
                      current_state,
                      last_state,
                      timestamp,
                      value=None,
                      metric=None):
        current_state_value = current_state["state"]
        last_state_value = last_state["state"]
        last_state["state"] = current_state_value

        if current_state.get("event_timestamp") is None:
            current_state["event_timestamp"] = timestamp

        event = {
            "trigger_id": self.id,
            "state": current_state_value,
            "old_state": last_state_value,
            "timestamp": timestamp,
            "metric": metric
        }

        if current_state_value == last_state_value:
            remind_interval = config.BAD_STATES_REMINDER.get(current_state_value)
            if remind_interval is None or timestamp - last_state.get("event_timestamp", timestamp) < remind_interval:
                if not last_state.get("suppressed") or current_state_value == state.OK:
                    raise StopIteration
            else:
                event["msg"] = "This metric has been in bad state for more than %s hours - please, fix." % \
                               (remind_interval / 3600)
        current_state["event_timestamp"] = timestamp
        last_state["event_timestamp"] = timestamp
        if value is not None:
            event["value"] = value
        current_state["suppressed"] = False
        last_state["suppressed"] = False
        if self.isSchedAllows(timestamp):
            state_maintenance = current_state.get("maintenance", 0)
            if self.maintenance >= timestamp:
                current_state["suppressed"] = True
                log.msg("Event %s suppressed due to maintenance until %s." %
                        (event, datetime.fromtimestamp(self.maintenance)))
            elif state_maintenance >= timestamp:
                current_state["suppressed"] = True
                log.msg("Event %s suppressed due to metric %s maintenance until %s." %
                        (event, metric, datetime.fromtimestamp(state_maintenance)))
            else:
                log.msg("Writing new event: %s" % event)
                yield self.db.pushEvent(event)
        else:
            current_state["suppressed"] = True
            log.msg("Event %s suppressed due to trigger schedule" % str(event))

    def isSchedAllows(self, ts):
        sched = self.struct.get('sched')
        if sched is None:
            return True

        timestamp = ts - ts % 60 - sched["tzOffset"] * 60
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
