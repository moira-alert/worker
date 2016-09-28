import os
import sys
sys.path.insert(0,
                os.path.abspath(
                    os.path.join(
                        os.path.abspath(
                            os.path.dirname(__file__)),
                        '../')))

import anyjson

from time import time

from twisted.internet import defer
from twisted.internet import reactor

from moira import config
from moira import logs
from moira.api.request import is_simple_target
from moira.db import Db
from moira.graphite import datalib
from moira.graphite.datalib import createRequestContext
from moira.graphite.evaluator import evaluateTarget


@defer.inlineCallbacks
def migrate_triggers(db):
    now = int(time())

    trigger_ids = yield db.getTriggers()
    logs.log.info("triggers count: %d" % len(trigger_ids))

    converted_triggers_count = 0
    simple_triggers_count = 0
    complex_triggers_count = 0
    failed_triggers_count = 0
    for trigger_id in trigger_ids:
        try:
            json, _ = yield db.getTrigger(trigger_id)
            if json is None:
                continue

            trigger = anyjson.deserialize(json)
            if "is_simple_trigger" in trigger:
                continue

            logs.log.info("recalculating for trigger %s (%s)" % (trigger_id, trigger.get("name")))
            context = createRequestContext(str(now - 600), str(now), allowRealTimeAlerting=True)
            if len(trigger["targets"]) != 1:
                is_simple_trigger = False
            else:
                yield evaluateTarget(context, trigger["targets"][0])
                is_simple_trigger = is_simple_target(context)
            trigger["is_simple_trigger"] = is_simple_trigger
            logs.log.info(str(trigger["is_simple_trigger"]))

            yield db.saveTrigger(trigger_id, trigger)

            converted_triggers_count += 1
            if is_simple_trigger:
                simple_triggers_count += 1
            else:
                complex_triggers_count += 1
        except Exception, e:
            failed_triggers_count += 1
            logs.log.error("conversion failed for trigger: %s" % e)

    logs.log.info("%d triggers converted, %d simple, %d complex, %d failed" %
                  (converted_triggers_count, simple_triggers_count, complex_triggers_count, failed_triggers_count))
    reactor.stop()


if __name__ == '__main__':
    config.read()
    logs.checker_worker()

    db = Db()
    datalib.db = db
    init = db.startService()
    init.addCallback(migrate_triggers)

    reactor.run()
