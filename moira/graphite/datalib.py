"""Copyright 2008 Orbitz WorldWide

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License."""

from moira.graphite.util import epoch
from moira.graphite.attime import parseATTime
from twisted.internet import defer

db = None


def createRequestContext(fromTime, toTime):
    return {'startTime': parseATTime(fromTime),
            'endTime': parseATTime(toTime),
            'localOnly': False,
            'template': None,
            'graphite_patterns': {},
            'metrics': set()}


class TimeSeries(list):

    def __init__(self, name, start, end, step, values, consolidate='average'):
        list.__init__(self, values)
        self.name = name
        self.start = start
        self.end = end
        self.step = step
        self.consolidationFunc = consolidate
        self.valuesPerPoint = 1
        self.options = {}

    def __iter__(self):
        if self.valuesPerPoint > 1:
            return self.__consolidatingGenerator(list.__iter__(self))
        else:
            return list.__iter__(self)

    def consolidate(self, valuesPerPoint):
        self.valuesPerPoint = int(valuesPerPoint)

    def __consolidatingGenerator(self, gen):
        buf = []
        for x in gen:
            buf.append(x)
            if len(buf) == self.valuesPerPoint:
                while None in buf:
                    buf.remove(None)
                if buf:
                    yield self.__consolidate(buf)
                    buf = []
                else:
                    yield None
        while None in buf:
            buf.remove(None)
        if buf:
            yield self.__consolidate(buf)
        else:
            yield None
        raise StopIteration

    def __consolidate(self, values):
        usable = [v for v in values if v is not None]
        if not usable:
            return None
        if self.consolidationFunc == 'sum':
            return sum(usable)
        if self.consolidationFunc == 'average':
            return float(sum(usable)) / len(usable)
        if self.consolidationFunc == 'max':
            return max(usable)
        if self.consolidationFunc == 'min':
            return min(usable)
        raise Exception("Invalid consolidation function!")

    def __repr__(self):
        return 'TimeSeries(name=%s, start=%s, end=%s, step=%s)' % (
            self.name, self.start, self.end, self.step)

    def getInfo(self):
        """Pickle-friendly representation of the series"""
        return {
            'name': self.name,
            'start': self.start,
            'end': self.end,
            'step': self.step,
            'values': list(self),
        }

# Data retrieval API


@defer.inlineCallbacks
def fetchData(requestContext, pathExpr):

    global db

    if db is None:
        raise Exception("Redis connection is not initialized")

    startTime = int(epoch(requestContext['startTime']))
    endTime = int(epoch(requestContext['endTime']))

    metrics = yield db.getPatternMetrics(pathExpr)
    seriesList = []
    buckets = {}
    max_bucket = None
    data = yield db.getMetricsValues(metrics, startTime, endTime)
    for i, metric in enumerate(metrics):
        requestContext['metrics'].add(metric)
        buckets[metric] = {}
        interval = yield db.getMetricRetention(metric, cache_key=metric, cache_ttl=60)
        buckets[metric]['interval'] = interval
        for timestamp, value in data[i]:
            bucket = (int)((timestamp - startTime) / interval)
            buckets[metric][bucket] = value
            if max_bucket is None or bucket > max_bucket:
                max_bucket = bucket

    for metric in buckets:
        values = []
        interval = buckets[metric]['interval']
        for bucket in range(0, (max_bucket or -1) + 1):
            values.append(buckets[metric].get(bucket))

        series = TimeSeries(
            metric,
            startTime,
            startTime + (max_bucket or 0) * interval,
            interval,
            values)

        series.pathExpression = pathExpr
        seriesList.append(series)

    if len(metrics) == 0:
        series = TimeSeries(pathExpr, startTime, startTime, 60, [])
        series.pathExpression = pathExpr
        seriesList.append(series)
    defer.returnValue(seriesList)
