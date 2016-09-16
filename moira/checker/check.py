from time import time
from twisted.internet import defer
from moira.graphite import datalib
from moira import config
from moira.checker import expression
from moira.checker import state
from moira.checker import event
from moira.logs import log


@defer.inlineCallbacks
def trigger(trigger, fromTime, now, cache_ttl):
    now = now or int(time())

    log.info("Checking trigger {id}", id=trigger.id)
    initialized = yield trigger.init(now, fromTime=fromTime)
    if not initialized:
        raise StopIteration

    if fromTime is None:
        fromTime = trigger.last_check.get("timestamp", now)

    fromTime = str(fromTime - (trigger.ttl or 600))
    requestContext = datalib.createRequestContext(fromTime, endTime=str(now), allowRealTimeAlerting=trigger.is_simple)

    check = {
        "metrics": trigger.last_check["metrics"].copy(),
        "state": state.OK,
        "timestamp": now,
        "score": trigger.last_check.get("score")
    }

    try:
        time_series = yield trigger.get_timeseries(requestContext)

        for metric in requestContext['metrics']:
            yield trigger.db.cleanupMetricValues(metric, now - config.METRICS_TTL,
                                                 cache_key=metric, cache_ttl=cache_ttl)

        if not time_series:
            if trigger.ttl:
                check["state"] = trigger.ttl_state
                check["msg"] = "Trigger has no metrics"
                yield event.compare_states(trigger, check, trigger.last_check, now)
        else:

            for t_series in time_series.values():
                for tN in t_series:
                    if not tN.stub:
                        check["metrics"][tN.name] = tN.last_state.copy()

            for t1 in time_series[1]:

                log.debug("Checking timeserie {name}: {values}", name=t1.name, values=list(t1))
                log.debug("Checking interval: {start} - {end} ({duration}s), step: {step}",
                          start=t1.start, end=t1.end, step=t1.step, duration=t1.end - t1.start)
                metric_state = check["metrics"].get(t1.name)
                if not metric_state:
                    log.debug("No metric state for {name}.", name=t1.name)
                    continue

                checkpoint = max(t1.last_state["timestamp"] - config.CHECKPOINT_GAP,
                                 metric_state.get("event_timestamp", 0))
                log.debug("Checkpoint for {name}: {checkpoint}", name=t1.name, checkpoint=checkpoint)

                for value_timestamp in xrange(t1.start, now + t1.step, t1.step):

                    if value_timestamp <= checkpoint:
                        continue

                    expression_values = time_series.get_expression_values(t1, value_timestamp)

                    t1_value = expression_values["t1"]

                    log.debug("values for ts {timestamp}: {values}",
                              timestamp=value_timestamp, values=expression_values)
                    if None in expression_values.values():
                        continue

                    expression_values.update({'warn_value': trigger.struct.get('warn_value'),
                                              'error_value': trigger.struct.get('error_value'),
                                              'PREV_STATE': metric_state['state']})

                    expression_state = expression.getExpression(trigger.struct.get('expression'),
                                                                **expression_values)

                    time_series.update_state(t1, check, expression_state, expression_values, value_timestamp)

                    yield event.compare_states(trigger, metric_state, t1.last_state,
                                               value_timestamp, value=t1_value,
                                               metric=t1.name)

                # compare with last_check timestamp in case if we have not run checker for a long time
                if trigger.ttl and metric_state["timestamp"] + trigger.ttl < trigger.last_check["timestamp"]:
                    log.info("Metric {name} TTL expired for state {state}", name=t1.name, state=metric_state)
                    if trigger.ttl_state == state.DEL and metric_state.get("event_timestamp") is not None:
                        log.info("Remove metric {name}", name=t1.name)
                        del check["metrics"][t1.name]
                        for tN, tName in time_series.other_targets_names.iteritems():
                            log.info("Remove metric {name}", name=tName)
                            del check["metrics"][tName]
                        for pattern in trigger.struct.get("patterns"):
                            yield trigger.db.delPatternMetrics(pattern)
                        continue
                    time_series.update_state(t1, check, state.to_metric_state(trigger.ttl_state), None,
                                             trigger.last_check["timestamp"] - trigger.ttl)
                    yield event.compare_states(trigger, metric_state, t1.last_state, metric_state["timestamp"],
                                               metric=t1.name)

    except StopIteration:
        raise
    except Exception as e:
        log.error("Trigger check failed: {e}", e=e)
        check["state"] = state.EXCEPTION
        check["msg"] = "Trigger evaluation exception"
        yield event.compare_states(trigger, check, trigger.last_check, now)
    scores = sum(map(lambda m: state.SCORES[m["state"]], check["metrics"].itervalues()))
    check["score"] = scores + state.SCORES[check["state"]]
    yield trigger.db.setTriggerLastCheck(trigger.id, check)
