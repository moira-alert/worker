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
def extract(value):
    parts = value.split()
    return float(parts[1])


@defer.inlineCallbacks
def fetchData(requestContext, pathExpr):

    global db

    if db is None:
        raise Exception("Redis connection is not initialized")

    startTime = int(epoch(requestContext['startTime']))
    endTime = int(epoch(requestContext['endTime']))
    seriesList = []

    metrics = list((yield db.getPatternMetrics(pathExpr)))

    if len(metrics) == 0:
        series = TimeSeries(pathExpr, startTime, startTime, 60, [])
        series.pathExpression = pathExpr
        seriesList.append(series)
    else:
        first_metric = metrics[0]
        retention = yield db.getMetricRetention(first_metric, cache_key=first_metric, cache_ttl=60)
        data = yield db.getMetricsValues(metrics, startTime, ("" if requestContext.get('delta') is None else "(") + str(endTime))
        for i, metric in enumerate(metrics):
            points = {}
            for value, timestamp in data[i]:
                bucket = (int)((timestamp - startTime) / retention)
                points[bucket] = extract(value)

            values = [points.get((int)((timestamp - startTime) / retention)) for timestamp in xrange(startTime, endTime + retention, retention)]

            series = TimeSeries(
                metric,
                startTime,
                endTime,
                retention,
                values)
            series.pathExpression = pathExpr
            seriesList.append(series)

    defer.returnValue(seriesList)
