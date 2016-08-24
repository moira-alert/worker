from datetime import datetime, timedelta

from moira.graphite.evaluator import evaluateTarget
from twisted.internet import defer

from moira.checker import state
from moira.checker import check
from moira.checker.timeseries import TargetTimeSeries


class Trigger(object):

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
        begin = (fromTime or now) - 3600
        if self.last_check is None:
            self.last_check = {
                "metrics": {},
                "state": state.NODATA,
                "timestamp": begin
            }
        if self.last_check.get("timestamp") is None:
            self.last_check["timestamp"] = begin
        defer.returnValue(True)

    @defer.inlineCallbacks
    def get_timeseries(self, requestContext):
        targets = self.struct.get("targets", [])
        target_time_series = TargetTimeSeries()
        target_number = 1

        for target in targets:
            time_series = yield evaluateTarget(requestContext, target)

            if target_number > 1:
                if len(time_series) == 1:
                    target_time_series.other_targets_names["t%s" % target_number] = time_series[0].name
                elif not time_series:
                    raise Exception("Target #%s has no timeseries" % target_number)
                else:
                    raise Exception("Target #%s has more than one timeseries" % target_number)

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
        yield check.trigger(self, fromTime, now, cache_ttl)

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
