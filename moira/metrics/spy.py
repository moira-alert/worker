from datetime import datetime, timedelta


def get_total_seconds(td):
    if hasattr(timedelta, 'total_seconds'):
        return td.total_seconds()
    else:
        return int(
            (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10 ** 6) / 10 ** 6)


class Spy(object):

    def __init__(self):
        self.eventList = []

    def trim_event_list(self, timestamp):
        while self.eventList:
            first_event = self.eventList[0]
            time_delta = timestamp - first_event['timestamp']
            if get_total_seconds(time_delta) > 60:
                del self.eventList[0]
            else:
                return

    def report(self, size):
        now = datetime.now()
        self.trim_event_list(now)
        if self.eventList:
            last_batch = self.eventList[-1]
            time_delta = now - last_batch['timestamp']
            if get_total_seconds(time_delta) < 10:
                last_batch['sum'] += size
                last_batch['count'] += 1
                return
        self.eventList.append({'timestamp': now, 'sum': size, 'count': 1})

    def get_metrics(self):
        now = datetime.now()
        count = 0
        summary = 0
        for batch in self.eventList:
            timedelta = now - batch['timestamp']
            if get_total_seconds(timedelta) > 60:
                break
            count += batch['count']
            summary += batch['sum']
        return {"sum": summary, "count": count}


TRIGGER_CHECK = Spy()
TRIGGER_CHECK_ERRORS = Spy()
