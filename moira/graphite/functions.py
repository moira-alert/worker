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

import math
import random
import re
import time

from datetime import datetime, timedelta
from itertools import izip, imap

from moira.graphite.attime import parseTimeOffset, parseATTime
from moira.graphite.util import epoch

from moira.graphite.datalib import TimeSeries
from twisted.internet import defer
from twisted.internet.defer import inlineCallbacks, returnValue
from functools import reduce

NAN = float('NaN')
INF = float('inf')
DAY = 86400
HOUR = 3600
MINUTE = 60

UnitSystems = {
    'binary': (
        ('Pi', 1024.0 ** 5),
        ('Ti', 1024.0 ** 4),
        ('Gi', 1024.0 ** 3),
        ('Mi', 1024.0 ** 2),
        ('Ki', 1024.0)),
    'si': (
        ('P', 1000.0 ** 5),
        ('T', 1000.0 ** 4),
        ('G', 1000.0 ** 3),
        ('M', 1000.0 ** 2),
        ('K', 1000.0)),
    'sec': (
        ('Y', 60 * 60 * 24 * 365),
        ('M', 60 * 60 * 24 * 30),
        ('D', 60 * 60 * 24),
        ('H', 60 * 60),
        ('m', 60)),
    'msec': (
        ('Y', 60 * 60 * 24 * 365 * 1000),
        ('M', 60 * 60 * 24 * 30 * 1000),
        ('D', 60 * 60 * 24 * 1000),
        ('H', 60 * 60 * 1000),
        ('m', 60 * 1000),
        ('s', 1000)),
    'none': [],
}


def format_units(v, step=None, system="si"):
    """Format the given value in standardized units.

    ``system`` is either 'binary' or 'si'

    For more info, see:
      http://en.wikipedia.org/wiki/SI_prefix
      http://en.wikipedia.org/wiki/Binary_prefix
    """

    if step is None:
        condition = lambda size: abs(v) >= size
    else:
        condition = lambda size: abs(v) >= size and step >= size

    for prefix, size in UnitSystems[system]:
        if condition(size):
            v2 = v / size
            if (v2 - math.floor(v2)) < 0.00000000001 and v > 1:
                v2 = math.floor(v2)
            return v2, prefix

    if (v - math.floor(v)) < 0.00000000001 and v > 1:
        v = math.floor(v)
    return v, ""

# Utility functions


def safeSum(values):
    safeValues = [v for v in values if v is not None]
    if safeValues:
        return sum(safeValues)


def safeDiff(values):
    safeValues = [v for v in values if v is not None]
    if safeValues:
        values = map(lambda x: x * -1, safeValues[1:])
        values.insert(0, safeValues[0])
        return sum(values)


def safeLen(values):
    return len([v for v in values if v is not None])


def safeDiv(a, b):
    if a is None:
        return None
    if b in (0, None):
        return None
    return float(a) / float(b)


def safePow(a, b):
    if a is None:
        return None
    try:
        result = math.pow(a, b)
    except ValueError:
        return None
    return result


def safeMul(*factors):
    if None in factors:
        return None

    factors = [float(x) for x in factors]
    product = reduce(lambda x, y: x * y, factors)
    return product


def safeSubtract(a, b):
    if a is None or b is None:
        return None
    return float(a) - float(b)


def safeAvg(a):
    return safeDiv(safeSum(a), safeLen(a))


def safeStdDev(a):
    sm = safeSum(a)
    ln = safeLen(a)
    avg = safeDiv(sm, ln)
    sum = 0
    safeValues = [v for v in a if v is not None]
    for val in safeValues:
        sum = sum + (val - avg) * (val - avg)
    return math.sqrt(sum / ln)


def safeLast(values):
    for v in reversed(values):
        if v is not None:
            return v


def safeMin(values):
    safeValues = [v for v in values if v is not None]
    if safeValues:
        return min(safeValues)


def safeMax(values):
    safeValues = [v for v in values if v is not None]
    if safeValues:
        return max(safeValues)


def safeMap(function, values):
    safeValues = [v for v in values if v is not None]
    if safeValues:
        return [function(x) for x in values]


def safeAbs(value):
    if value is None:
        return None
    return abs(value)


def safeIsNotEmpty(values):
    safeValues = [v for v in values if v is not None]
    return len(safeValues) > 0

# Greatest common divisor


def gcd(a, b):
    if b == 0:
        return a
    return gcd(b, a % b)

# Least common multiple


def lcm(a, b):
    if a == b:
        return a
    if a < b:
        (a, b) = (b, a)  # ensure a > b
    return a / gcd(a, b) * b


def normalize(seriesLists):
    if seriesLists:
        seriesList = reduce(lambda L1, L2: L1 + L2, seriesLists)
        if seriesList:
            step = reduce(lcm, [s.step for s in seriesList])
            for s in seriesList:
                s.consolidate(step / s.step)
            start = min([s.start for s in seriesList])
            end = max([s.end for s in seriesList])
            end -= (end - start) % step
            return (seriesList, start, end, step)
    raise NormalizeEmptyResultError()


class NormalizeEmptyResultError(Exception):
    # throw error for normalize() when empty
    pass


def formatPathExpressions(seriesList):
    # remove duplicates
    pathExpressions = []
    [pathExpressions.append(s.pathExpression)
     for s in seriesList if not pathExpressions.count(s.pathExpression)]
    return ','.join(pathExpressions)

# Series Functions

# NOTE: Some of the functions below use izip, which may be problematic.
# izip stops when it hits the end of the shortest series
# in practice this *shouldn't* matter because all series will cover
# the same interval, despite having possibly different steps...


@inlineCallbacks
def sumSeries(requestContext, *seriesLists):
    """
    Short form: sum()

    This will add metrics together and return the sum at each datapoint. (See
    integral for a sum over time)

    Example:

    .. code-block:: none

      &target=sum(company.server.application*.requestsHandled)

    This would show the sum of all requests handled per minute (provided
    requestsHandled are collected once a minute).   If metrics with different
    retention rates are combined, the coarsest metric is graphed, and the sum
    of the other metrics is averaged for the metrics with finer retention rates.

    """
    yield defer.succeed(None)
    try:
        (seriesList, start, end, step) = normalize(seriesLists)
    except:
        returnValue([])
    name = "sumSeries(%s)" % formatPathExpressions(seriesList)
    values = (safeSum(row) for row in izip(*seriesList))
    series = TimeSeries(name, start, end, step, values)
    series.pathExpression = name
    returnValue([series])


@inlineCallbacks
def sumSeriesWithWildcards(requestContext, seriesList, *position):  # XXX
    """
    Call sumSeries after inserting wildcards at the given position(s).

    Example:

    .. code-block:: none

      &target=sumSeriesWithWildcards(host.cpu-[0-7].cpu-{user,system}.value, 1)

    This would be the equivalent of
    ``target=sumSeries(host.cpu-[0-7].cpu-user.value)&target=sumSeries(host.cpu-[0-7].cpu-system.value)``

    """
    yield defer.succeed(None)
    if isinstance(position, int):
        positions = [position]
    else:
        positions = position

    newSeries = {}
    newNames = list()

    for series in seriesList:
        newname = '.'.join(map(lambda x: x[1], filter(
            lambda i: i[0] not in positions, enumerate(series.name.split('.')))))
        if newname in newSeries:
            newSeries[newname] = (yield sumSeries(
                requestContext, (series, newSeries[newname])))[0]
        else:
            newSeries[newname] = series
            newNames.append(newname)
        newSeries[newname].name = newname

    returnValue([newSeries[name] for name in newNames])


@inlineCallbacks
def averageSeriesWithWildcards(requestContext, seriesList, *position):  # XXX
    """
    Call averageSeries after inserting wildcards at the given position(s).

    Example:

    .. code-block:: none

      &target=averageSeriesWithWildcards(host.cpu-[0-7].cpu-{user,system}.value, 1)

    This would be the equivalent of
    ``target=averageSeries(host.*.cpu-user.value)&target=averageSeries(host.*.cpu-system.value)``

    """
    yield defer.succeed(None)
    if isinstance(position, int):
        positions = [position]
    else:
        positions = position
    result = []
    matchedList = {}
    for series in seriesList:
        newname = '.'.join(map(lambda x: x[1], filter(
            lambda i: i[0] not in positions, enumerate(series.name.split('.')))))
        if newname not in matchedList:
            matchedList[newname] = []
        matchedList[newname].append(series)
    for name in matchedList.keys():
        result.append((yield averageSeries(requestContext, (matchedList[name])))[0])
        result[-1].name = name
    returnValue(result)


@inlineCallbacks
def multiplySeriesWithWildcards(requestContext, seriesList, *position):  # XXX
    """
    Call multiplySeries after inserting wildcards at the given position(s).

    Example:

    .. code-block:: none

      &target=multiplySeriesWithWildcards([web.host-[0-7].avg-response.value, web.host-[0-7].total-request.value], 2,3)

    This would be the equivalent of
    ``target=multiplySeries(web.host-0.avg-response.value, web.host-0.total-request.value)&target=multiplySeries(
    web.host-1.avg-response.value, web.host-1.total-request.value)...``

    """
    yield defer.succeed(None)
    if isinstance(position, int):
        positions = [position]
    else:
        positions = position

    newSeries = {}
    newNames = list()

    for series in seriesList:
        newname = '.'.join(map(lambda x: x[1], filter(
            lambda i: i[0] not in positions, enumerate(series.name.split('.')))))
        if newname in newSeries:
            newSeries[newname] = multiplySeries(
                requestContext, (newSeries[newname], series))[0]
        else:
            newSeries[newname] = series
            newNames.append(newname)
        newSeries[newname].name = newname
    returnValue([newSeries[name] for name in newNames])


@inlineCallbacks
def diffSeries(requestContext, *seriesLists):
    """
    Subtracts series 2 through n from series 1.

    Example:

    .. code-block:: none

      &target=diffSeries(service.connections.total,service.connections.failed)

    To diff a series and a constant, one should use offset instead of (or in
    addition to) diffSeries

    Example:

    .. code-block:: none

      &target=offset(service.connections.total,-5)

      &target=offset(diffSeries(service.connections.total,service.connections.failed),-4)

    """
    yield defer.succeed(None)
    (seriesList, start, end, step) = normalize(seriesLists)
    name = "diffSeries(%s)" % formatPathExpressions(seriesList)
    values = (safeDiff(row) for row in izip(*seriesList))
    series = TimeSeries(name, start, end, step, values)
    series.pathExpression = name
    returnValue([series])


@inlineCallbacks
def averageSeries(requestContext, *seriesLists):
    """
    Short Alias: avg()

    Takes one metric or a wildcard seriesList.
    Draws the average value of all metrics passed at each time.

    Example:

    .. code-block:: none

      &target=averageSeries(company.server.*.threads.busy)

    """
    yield defer.succeed(None)
    (seriesList, start, end, step) = normalize(seriesLists)
    name = "averageSeries(%s)" % formatPathExpressions(seriesList)
    values = (safeDiv(safeSum(row), safeLen(row)) for row in izip(*seriesList))
    series = TimeSeries(name, start, end, step, values)
    series.pathExpression = name
    returnValue([series])


@inlineCallbacks
def stddevSeries(requestContext, *seriesLists):
    """

    Takes one metric or a wildcard seriesList.
    Draws the standard deviation of all metrics passed at each time.

    Example:

    .. code-block:: none

      &target=stddevSeries(company.server.*.threads.busy)

    """
    yield defer.succeed(None)
    (seriesList, start, end, step) = normalize(seriesLists)
    name = "stddevSeries(%s)" % formatPathExpressions(seriesList)
    values = (safeStdDev(row) for row in izip(*seriesList))
    series = TimeSeries(name, start, end, step, values)
    series.pathExpression = name
    returnValue([series])


@inlineCallbacks
def minSeries(requestContext, *seriesLists):
    """
    Takes one metric or a wildcard seriesList.
    For each datapoint from each metric passed in, pick the minimum value and graph it.

    Example:

    .. code-block:: none

      &target=minSeries(Server*.connections.total)
    """
    yield defer.succeed(None)
    (seriesList, start, end, step) = normalize(seriesLists)
    name = "minSeries(%s)" % formatPathExpressions(seriesList)
    values = (safeMin(row) for row in izip(*seriesList))
    series = TimeSeries(name, start, end, step, values)
    series.pathExpression = name
    returnValue([series])


@inlineCallbacks
def maxSeries(requestContext, *seriesLists):
    """
    Takes one metric or a wildcard seriesList.
    For each datapoint from each metric passed in, pick the maximum value and graph it.

    Example:

    .. code-block:: none

      &target=maxSeries(Server*.connections.total)

    """
    yield defer.succeed(None)
    (seriesList, start, end, step) = normalize(seriesLists)
    name = "maxSeries(%s)" % formatPathExpressions(seriesList)
    values = (safeMax(row) for row in izip(*seriesList))
    series = TimeSeries(name, start, end, step, values)
    series.pathExpression = name
    returnValue([series])


@inlineCallbacks
def rangeOfSeries(requestContext, *seriesLists):
    """
    Takes a wildcard seriesList.
    Distills down a set of inputs into the range of the series

    Example:

    .. code-block:: none

        &target=rangeOfSeries(Server*.connections.total)

    """
    yield defer.succeed(None)
    (seriesList, start, end, step) = normalize(seriesLists)
    name = "rangeOfSeries(%s)" % formatPathExpressions(seriesList)
    values = (safeSubtract(max(row), min(row)) for row in izip(*seriesList))
    series = TimeSeries(name, start, end, step, values)
    series.pathExpression = name
    returnValue([series])


@inlineCallbacks
def percentileOfSeries(requestContext, seriesList, n, interpolate=False):
    """
    percentileOfSeries returns a single series which is composed of the n-percentile
    values taken across a wildcard series at each point. Unless `interpolate` is
    set to True, percentile values are actual values contained in one of the
    supplied series.
    """
    yield defer.succeed(None)
    if n <= 0:
        raise ValueError(
            'The requested percent is required to be greater than 0')

    name = 'percentileOfSeries(%s,%g)' % (seriesList[0].pathExpression, n)
    (start, end, step) = normalize([seriesList])[1:]
    values = [_getPercentile(row, n, interpolate) for row in izip(*seriesList)]
    resultSeries = TimeSeries(name, start, end, step, values)
    resultSeries.pathExpression = name

    returnValue([resultSeries])


@inlineCallbacks
def keepLastValue(requestContext, seriesList, limit=INF):
    """
    Takes one metric or a wildcard seriesList, and optionally a limit to the number
    of 'None' values to skip over.
    Continues the line with the last received value when gaps ('None' values) appear
    in your data, rather than breaking your line.

    Example:

    .. code-block:: none

      &target=keepLastValue(Server01.connections.handled)
      &target=keepLastValue(Server01.connections.handled, 10)

    """
    yield defer.succeed(None)
    for series in seriesList:
        series.name = "keepLastValue(%s)" % (series.name)
        series.pathExpression = series.name
        consecutiveNones = 0
        for i, value in enumerate(series):
            series[i] = value

            # No 'keeping' can be done on the first value because we have no idea
            # what came before it.
            if i == 0:
                continue

            if value is None:
                consecutiveNones += 1
            else:
                if 0 < consecutiveNones <= limit:
                    # If a non-None value is seen before the limit of Nones is hit,
                    # backfill all the missing datapoints with the last known
                    # value.
                    for index in xrange(i - consecutiveNones, i):
                        series[index] = series[i - consecutiveNones - 1]

                consecutiveNones = 0

        # If the series ends with some None values, try to backfill a bit to
        # cover it.
        if 0 < consecutiveNones < limit:
            for index in xrange(len(series) - consecutiveNones, len(series)):
                series[index] = series[len(series) - consecutiveNones - 1]

    returnValue(seriesList)


@inlineCallbacks
def changed(requestContext, seriesList):
    """
    Takes one metric or a wildcard seriesList.
    Output 1 when the value changed, 0 when null or the same

    Example:

    .. code-block:: none

      &target=changed(Server01.connections.handled)

    """
    yield defer.succeed(None)
    for series in seriesList:
        series.name = "changed(%s)" % (series.name)
        series.pathExpression = series.name
        previous = None
        for i, value in enumerate(series):
            if previous is None:
                previous = value
                series[i] = 0
            elif value is not None and previous != value:
                series[i] = 1
                previous = value
            else:
                series[i] = 0
    returnValue(seriesList)


@inlineCallbacks
def asPercent(requestContext, seriesList, total=None):
    """

    Calculates a percentage of the total of a wildcard series. If `total` is specified,
    each series will be calculated as a percentage of that total. If `total` is not specified,
    the sum of all points in the wildcard series will be used instead.

    The `total` parameter may be a single series or a numeric value.

    Example:

    .. code-block:: none

      &target=asPercent(Server01.connections.{failed,succeeded}, Server01.connections.attempted)
      &target=asPercent(apache01.threads.busy,1500)
      &target=asPercent(Server01.cpu.*.jiffies)

    """
    yield defer.succeed(None)
    normalize([seriesList])

    if total is None:
        totalValues = [safeSum(row) for row in izip(*seriesList)]
        totalText = None  # series.pathExpression
    elif isinstance(total, list):
        if len(total) != 1:
            raise ValueError(
                "asPercent second argument must reference exactly 1 series")
        normalize([seriesList, total])
        totalValues = total[0]
        totalText = totalValues.name
    else:
        totalValues = [total] * len(seriesList[0])
        totalText = str(total)

    resultList = []
    for series in seriesList:
        resultValues = [
            safeMul(
                safeDiv(
                    val, totalVal), 100.0) for val, totalVal in izip(
                series, totalValues)]

        name = "asPercent(%s, %s)" % (
            series.name, totalText or series.pathExpression)
        resultSeries = TimeSeries(
            name,
            series.start,
            series.end,
            series.step,
            resultValues)
        resultSeries.pathExpression = name
        resultList.append(resultSeries)

    returnValue(resultList)


@inlineCallbacks
def divideSeries(requestContext, dividendSeriesList, divisorSeries):
    """
    Takes a dividend metric and a divisor metric and draws the division result.
    A constant may *not* be passed. To divide by a constant, use the scale()
    function (which is essentially a multiplication operation) and use the inverse
    of the dividend. (Division by 8 = multiplication by 1/8 or 0.125)

    Example:

    .. code-block:: none

      &target=divideSeries(Series.dividends,Series.divisors)


    """
    yield defer.succeed(None)
    if len(divisorSeries) != 1:
        raise ValueError(
            "divideSeries second argument must reference exactly 1 series")

    divisorSeries = divisorSeries[0]
    results = []

    for dividendSeries in dividendSeriesList:
        name = "divideSeries(%s,%s)" % (
            dividendSeries.name, divisorSeries.name)
        bothSeries = (dividendSeries, divisorSeries)
        step = reduce(lcm, [s.step for s in bothSeries])

        for s in bothSeries:
            s.consolidate(step / s.step)

        start = min([s.start for s in bothSeries])
        end = max([s.end for s in bothSeries])
        end -= (end - start) % step

        values = (safeDiv(v1, v2) for v1, v2 in izip(*bothSeries))

        quotientSeries = TimeSeries(name, start, end, step, values)
        quotientSeries.pathExpression = name
        results.append(quotientSeries)

    returnValue(results)


@inlineCallbacks
def multiplySeries(requestContext, *seriesLists):
    """
    Takes two or more series and multiplies their points. A constant may not be
    used. To multiply by a constant, use the scale() function.

    Example:

    .. code-block:: none

      &target=multiplySeries(Series.dividends,Series.divisors)


    """

    yield defer.succeed(None)
    (seriesList, start, end, step) = normalize(seriesLists)

    if len(seriesList) == 1:
        returnValue(seriesList)

    name = "multiplySeries(%s)" % ','.join([s.name for s in seriesList])
    product = imap(lambda x: safeMul(*x), izip(*seriesList))
    resultSeries = TimeSeries(name, start, end, step, product)
    resultSeries.pathExpression = name
    returnValue([resultSeries])


@inlineCallbacks
def weightedAverage(requestContext, seriesListAvg, seriesListWeight, node):
    """
    Takes a series of average values and a series of weights and
    produces a weighted average for all values.

    The corresponding values should share a node as defined
    by the node parameter, 0-indexed.

    Example:

    .. code-block:: none

      &target=weightedAverage(*.transactions.mean,*.transactions.count,0)

    """

    yield defer.succeed(None)
    sortedSeries = {}

    for seriesAvg, seriesWeight in izip(seriesListAvg, seriesListWeight):
        key = seriesAvg.name.split(".")[node]
        if key not in sortedSeries:
            sortedSeries[key] = {}

        sortedSeries[key]['avg'] = seriesAvg
        key = seriesWeight.name.split(".")[node]
        if key not in sortedSeries:
            sortedSeries[key] = {}
        sortedSeries[key]['weight'] = seriesWeight

    productList = []

    for key in sortedSeries.keys():
        if 'weight' not in sortedSeries[key]:
            continue
        if 'avg' not in sortedSeries[key]:
            continue

        seriesWeight = sortedSeries[key]['weight']
        seriesAvg = sortedSeries[key]['avg']

        productValues = [
            safeMul(
                val1, val2) for val1, val2 in izip(
                seriesAvg, seriesWeight)]
        name = 'product(%s,%s)' % (seriesWeight.name, seriesAvg.name)
        productSeries = TimeSeries(
            name,
            seriesAvg.start,
            seriesAvg.end,
            seriesAvg.step,
            productValues)
        productSeries.pathExpression = name
        productList.append(productSeries)

    sumProducts = sumSeries(requestContext, productList)[0]
    sumWeights = sumSeries(requestContext, seriesListWeight)[0]

    resultValues = [
        safeDiv(
            val1, val2) for val1, val2 in izip(
            sumProducts, sumWeights)]
    name = "weightedAverage(%s, %s)" % (','.join(set(s.pathExpression for s in seriesListAvg)), ','.join(
        set(s.pathExpression for s in seriesListWeight)))
    resultSeries = TimeSeries(
        name,
        sumProducts.start,
        sumProducts.end,
        sumProducts.step,
        resultValues)
    resultSeries.pathExpression = name
    returnValue(resultSeries)


@inlineCallbacks
def movingMedian(requestContext, seriesList, windowSize):
    """
    Graphs the moving median of a metric (or metrics) over a fixed number of
    past points, or a time interval.

    Takes one metric or a wildcard seriesList followed by a number N of datapoints
    or a quoted string with a length of time like '1hour' or '5min' (See ``from /
    until`` in the render\_api_ for examples of time formats). Graphs the
    median of the preceding datapoints for each point on the graph. All
    previous datapoints are set to None at the beginning of the graph.

    Example:

    .. code-block:: none

      &target=movingMedian(Server.instance01.threads.busy,10)
      &target=movingMedian(Server.instance*.threads.idle,'5min')

    """
    yield defer.succeed(None)
    if not seriesList:
        returnValue([])
    windowInterval = None
    if isinstance(windowSize, basestring):
        delta = parseTimeOffset(windowSize)
        windowInterval = abs(delta.seconds + (delta.days * 86400))

    if windowInterval:
        bootstrapSeconds = windowInterval
    else:
        bootstrapSeconds = max([s.step for s in seriesList]) * int(windowSize)

    requestContext['delta'] = bootstrapSeconds
    bootstrapList = yield _fetchWithBootstrap(requestContext, seriesList, seconds=bootstrapSeconds)
    result = []

    for bootstrap, series in zip(bootstrapList, seriesList):
        if windowInterval:
            windowPoints = windowInterval / series.step
        else:
            windowPoints = int(windowSize)

        if isinstance(windowSize, basestring):
            newName = 'movingMedian(%s,"%s")' % (series.name, windowSize)
        else:
            newName = "movingMedian(%s,%d)" % (series.name, windowPoints)
        newSeries = TimeSeries(
            newName,
            series.start,
            series.end,
            series.step,
            [])
        newSeries.pathExpression = newName

        offset = len(bootstrap) - len(series)
        for i in range(len(series)):
            window = bootstrap[i + offset - windowPoints:i + offset]
            nonNull = [v for v in window if v is not None]
            if nonNull:
                m_index = len(nonNull) / 2
                newSeries.append(sorted(nonNull)[m_index])
            else:
                newSeries.append(None)
        result.append(newSeries)

    returnValue(result)


@inlineCallbacks
def scale(requestContext, seriesList, factor):
    """
    Takes one metric or a wildcard seriesList followed by a constant, and multiplies the datapoint
    by the constant provided at each point.

    Example:

    .. code-block:: none

      &target=scale(Server.instance01.threads.busy,10)
      &target=scale(Server.instance*.threads.busy,10)

    """
    yield defer.succeed(None)
    for series in seriesList:
        series.name = "scale(%s,%g)" % (series.name, float(factor))
        series.pathExpression = series.name
        for i, value in enumerate(series):
            series[i] = safeMul(value, factor)
    returnValue(seriesList)


@inlineCallbacks
def scaleToSeconds(requestContext, seriesList, seconds):
    """
    Takes one metric or a wildcard seriesList and returns "value per seconds" where
    seconds is a last argument to this functions.

    Useful in conjunction with derivative or integral function if you want
    to normalize its result to a known resolution for arbitrary retentions
    """

    yield defer.succeed(None)
    for series in seriesList:
        series.name = "scaleToSeconds(%s,%d)" % (series.name, seconds)
        series.pathExpression = series.name
        for i, value in enumerate(series):
            factor = seconds * 1.0 / series.step
            series[i] = safeMul(value, factor)
    returnValue(seriesList)


@inlineCallbacks
def pow(requestContext, seriesList, factor):
    """
    Takes one metric or a wildcard seriesList followed by a constant, and raises the datapoint
    by the power of the constant provided at each point.

    Example:

    .. code-block:: none

      &target=pow(Server.instance01.threads.busy,10)
      &target=pow(Server.instance*.threads.busy,10)

    """
    yield defer.succeed(None)
    for series in seriesList:
        series.name = "pow(%s,%g)" % (series.name, float(factor))
        series.pathExpression = series.name
        for i, value in enumerate(series):
            series[i] = safePow(value, factor)
    returnValue(seriesList)


@inlineCallbacks
def squareRoot(requestContext, seriesList):
    """
    Takes one metric or a wildcard seriesList, and computes the square root of each datapoint.

    Example:

    .. code-block:: none

      &target=squareRoot(Server.instance01.threads.busy)

    """
    yield defer.succeed(None)
    for series in seriesList:
        series.name = "squareRoot(%s)" % (series.name)
        for i, value in enumerate(series):
            series[i] = safePow(value, 0.5)
    returnValue(seriesList)


@inlineCallbacks
def invert(requestContext, seriesList):
    """
    Takes one metric or a wildcard seriesList, and inverts each datapoint (i.e. 1/x).

    Example:

    .. code-block:: none

      &target=invert(Server.instance01.threads.busy)

    """
    yield defer.succeed(None)
    for series in seriesList:
        series.name = "invert(%s)" % (series.name)
        for i, value in enumerate(series):
            series[i] = safePow(value, -1)
    returnValue(seriesList)


@inlineCallbacks
def absolute(requestContext, seriesList):
    """
    Takes one metric or a wildcard seriesList and applies the mathematical abs function to each
    datapoint transforming it to its absolute value.

    Example:

    .. code-block:: none

      &target=absolute(Server.instance01.threads.busy)
      &target=absolute(Server.instance*.threads.busy)
    """
    yield defer.succeed(None)
    for series in seriesList:
        series.name = "absolute(%s)" % (series.name)
        series.pathExpression = series.name
        for i, value in enumerate(series):
            series[i] = safeAbs(value)
    returnValue(seriesList)


def offset(requestContext, seriesList, factor):
    """
    Takes one metric or a wildcard seriesList followed by a constant, and adds the constant to
    each datapoint.

    Example:

    .. code-block:: none

      &target=offset(Server.instance01.threads.busy,10)

    """
    for series in seriesList:
        series.name = "offset(%s,%g)" % (series.name, float(factor))
        series.pathExpression = series.name
        for i, value in enumerate(series):
            if value is not None:
                series[i] = value + factor
    return seriesList


@inlineCallbacks
def offsetToZero(requestContext, seriesList):
    """
    Offsets a metric or wildcard seriesList by subtracting the minimum
    value in the series from each datapoint.

    Useful to compare different series where the values in each series
    may be higher or lower on average but you're only interested in the
    relative difference.

    An example use case is for comparing different round trip time
    results. When measuring RTT (like pinging a server), different
    devices may come back with consistently different results due to
    network latency which will be different depending on how many
    network hops between the probe and the device. To compare different
    devices in the same graph, the network latency to each has to be
    factored out of the results. This is a shortcut that takes the
    fastest response (lowest number in the series) and sets that to zero
    and then offsets all of the other datapoints in that series by that
    amount. This makes the assumption that the lowest response is the
    fastest the device can respond, of course the more datapoints that
    are in the series the more accurate this assumption is.

    Example:

    .. code-block:: none

      &target=offsetToZero(Server.instance01.responseTime)
      &target=offsetToZero(Server.instance*.responseTime)

    """
    yield defer.succeed(None)
    for series in seriesList:
        series.name = "offsetToZero(%s)" % (series.name)
        minimum = safeMin(series)
        for i, value in enumerate(series):
            if value is not None:
                series[i] = value - minimum
    returnValue(seriesList)


@inlineCallbacks
def movingAverage(requestContext, seriesList, windowSize, func='avg'):
    """
    Graphs the moving average of a metric (or metrics) over a fixed number of
    past points, or a time interval.

    Takes one metric or a wildcard seriesList followed by a number N of datapoints
    or a quoted string with a length of time like '1hour' or '5min' (See ``from /
    until`` in the render\_api_ for examples of time formats). Graphs the
    average of the preceding datapoints for each point on the graph. All
    previous datapoints are set to None at the beginning of the graph.

    Example:

    .. code-block:: none

      &target=movingAverage(Server.instance01.threads.busy,10)
      &target=movingAverage(Server.instance*.threads.idle,'5min')

    """
    t_funcs = {'avg': safeAvg, 'min': safeMin, 'max': safeMax}
    yield defer.succeed(None)
    if not seriesList:
        returnValue([])
    windowInterval = None
    if isinstance(windowSize, basestring):
        delta = parseTimeOffset(windowSize)
        windowInterval = abs(delta.seconds + (delta.days * 86400))

    if windowInterval:
        bootstrapSeconds = windowInterval
    else:
        bootstrapSeconds = max([s.step for s in seriesList]) * int(windowSize)

    requestContext['delta'] = bootstrapSeconds
    bootstrapList = yield _fetchWithBootstrap(requestContext, seriesList, seconds=bootstrapSeconds)
    result = []

    for bootstrap, series in zip(bootstrapList, seriesList):
        if windowInterval:
            windowPoints = windowInterval / series.step
        else:
            windowPoints = int(windowSize)

        if isinstance(windowSize, basestring):
            newName = 'movingAverage(%s,"%s")' % (series.name, windowSize)
        else:
            newName = "movingAverage(%s,%s)" % (series.name, windowSize)
        newSeries = TimeSeries(
            newName,
            series.start,
            series.end,
            series.step,
            [])
        newSeries.pathExpression = newName

        offset = len(bootstrap) - len(series)
        for i in range(len(series)):
            window = bootstrap[i + offset - windowPoints + 1:i + offset + 1]
            newSeries.append(t_funcs[func](window))

        result.append(newSeries)

    returnValue(result)


@inlineCallbacks
def cumulative(requestContext, seriesList, consolidationFunc='sum'):
    """
    Takes one metric or a wildcard seriesList, and an optional function.

    Valid functions are 'sum', 'average', 'min', and 'max'

    Sets the consolidation function to 'sum' for the given metric seriesList.

    Alias for :func:`consolidateBy(series, 'sum') <graphite.render.functions.consolidateBy>`

    .. code-block:: none

      &target=cumulative(Sales.widgets.largeBlue)

    """
    yield defer.succeed(None)
    result = yield consolidateBy(requestContext, seriesList, 'sum')
    returnValue(result)


@inlineCallbacks
def consolidateBy(requestContext, seriesList, consolidationFunc):
    """
    Takes one metric or a wildcard seriesList and a consolidation function name.

    Valid function names are 'sum', 'average', 'min', and 'max'

    When a graph is drawn where width of the graph size in pixels is smaller than
    the number of datapoints to be graphed, Graphite consolidates the values to
    to prevent line overlap. The consolidateBy() function changes the consolidation
    function from the default of 'average' to one of 'sum', 'max', or 'min'. This is
    especially useful in sales graphs, where fractional values make no sense and a 'sum'
    of consolidated values is appropriate.

    .. code-block:: none

      &target=consolidateBy(Sales.widgets.largeBlue, 'sum')
      &target=consolidateBy(Servers.web01.sda1.free_space, 'max')

    """
    yield defer.succeed(None)
    for series in seriesList:
        # datalib will throw an exception, so it's not necessary to validate
        # here
        series.consolidationFunc = consolidationFunc
        series.name = 'consolidateBy(%s,"%s")' % (
            series.name, series.consolidationFunc)
        series.pathExpression = series.name
    returnValue(seriesList)


@inlineCallbacks
def derivative(requestContext, seriesList):
    """
    This is the opposite of the integral function.  This is useful for taking a
    running total metric and calculating the delta between subsequent data points.

    This function does not normalize for periods of time, as a true derivative would.
    Instead see the perSecond() function to calculate a rate of change over time.

    Example:

    .. code-block:: none

      &target=derivative(company.server.application01.ifconfig.TXPackets)

    Each time you run ifconfig, the RX and TXPackets are higher (assuming there
    is network traffic.) By applying the derivative function, you can get an
    idea of the packets per minute sent or received, even though you're only
    recording the total.
    """
    yield defer.succeed(None)
    results = []
    for series in seriesList:
        newValues = []
        prev = None
        for val in series:
            if None in (prev, val):
                newValues.append(None)
                prev = val
                continue
            newValues.append(val - prev)
            prev = val
        newName = "derivative(%s)" % series.name
        newSeries = TimeSeries(
            newName,
            series.start,
            series.end,
            series.step,
            newValues)
        newSeries.pathExpression = newName
        results.append(newSeries)
    returnValue(results)


@inlineCallbacks
def perSecond(requestContext, seriesList, maxValue=None):
    """
    Derivative adjusted for the series time interval
    This is useful for taking a running total metric and showing how many requests
    per second were handled.

    Example:

    .. code-block:: none

      &target=perSecond(company.server.application01.ifconfig.TXPackets)

    Each time you run ifconfig, the RX and TXPackets are higher (assuming there
    is network traffic.) By applying the derivative function, you can get an
    idea of the packets per minute sent or received, even though you're only
    recording the total.
    """
    yield defer.succeed(None)
    results = []
    for series in seriesList:
        newValues = []
        prev = None
        for val in series:
            step = series.step
            if None in (prev, val):
                newValues.append(None)
                prev = val
                continue

            diff = val - prev
            if diff >= 0:
                newValues.append(diff / step)
            elif maxValue is not None and maxValue >= val:
                newValues.append(((maxValue - prev) + val + 1) / step)
            else:
                newValues.append(None)

            prev = val
        newName = "perSecond(%s)" % series.name
        newSeries = TimeSeries(
            newName,
            series.start,
            series.end,
            series.step,
            newValues)
        newSeries.pathExpression = newName
        results.append(newSeries)
    returnValue(results)


@inlineCallbacks
def integral(requestContext, seriesList):
    """
    This will show the sum over time, sort of like a continuous addition function.
    Useful for finding totals or trends in metrics that are collected per minute.

    Example:

    .. code-block:: none

      &target=integral(company.sales.perMinute)

    This would start at zero on the left side of the graph, adding the sales each
    minute, and show the total sales for the time period selected at the right
    side, (time now, or the time specified by '&until=').
    """
    yield defer.succeed(None)
    results = []
    for series in seriesList:
        newValues = []
        current = 0.0
        for val in series:
            if val is None:
                newValues.append(None)
            else:
                current += val
                newValues.append(current)
        newName = "integral(%s)" % series.name
        newSeries = TimeSeries(
            newName,
            series.start,
            series.end,
            series.step,
            newValues)
        newSeries.pathExpression = newName
        results.append(newSeries)
    returnValue(results)


@inlineCallbacks
def nonNegativeDerivative(requestContext, seriesList, maxValue=None):
    """
    Same as the derivative function above, but ignores datapoints that trend
    down.  Useful for counters that increase for a long time, then wrap or
    reset. (Such as if a network interface is destroyed and recreated by unloading
    and re-loading a kernel module, common with USB / WiFi cards.

    Example:

    .. code-block:: none

      &target=nonNegativederivative(company.server.application01.ifconfig.TXPackets)

    """
    yield defer.succeed(None)
    results = []

    for series in seriesList:
        newValues = []
        prev = None

        for val in series:
            if None in (prev, val):
                newValues.append(None)
                prev = val
                continue

            diff = val - prev
            if diff >= 0:
                newValues.append(diff)
            elif maxValue is not None and maxValue >= val:
                newValues.append((maxValue - prev) + val + 1)
            else:
                newValues.append(None)

            prev = val

        newName = "nonNegativeDerivative(%s)" % series.name
        newSeries = TimeSeries(
            newName,
            series.start,
            series.end,
            series.step,
            newValues)
        newSeries.pathExpression = newName
        results.append(newSeries)

    returnValue(results)


@inlineCallbacks
def stacked(requestContext, seriesLists, stackName='__DEFAULT__'):
    """
    Takes one metric or a wildcard seriesList and change them so they are
    stacked. This is a way of stacking just a couple of metrics without having
    to use the stacked area mode (that stacks everything). By means of this a mixed
    stacked and non stacked graph can be made

    It can also take an optional argument with a name of the stack, in case there is
    more than one, e.g. for input and output metrics.

    Example:

    .. code-block:: none

      &target=stacked(company.server.application01.ifconfig.TXPackets, 'tx')

    """
    yield defer.succeed(None)
    if 'totalStack' in requestContext:
        totalStack = requestContext['totalStack'].get(stackName, [])
    else:
        requestContext['totalStack'] = {}
        totalStack = []
    results = []
    for series in seriesLists:
        newValues = []
        for i in range(len(series)):
            if len(totalStack) <= i:
                totalStack.append(0)

            if series[i] is not None:
                totalStack[i] += series[i]
                newValues.append(totalStack[i])
            else:
                newValues.append(None)

        # Work-around for the case when legend is set
        if stackName == '__DEFAULT__':
            newName = "stacked(%s)" % series.name
        else:
            newName = series.name

        newSeries = TimeSeries(
            newName,
            series.start,
            series.end,
            series.step,
            newValues)
        newSeries.options['stacked'] = True
        newSeries.pathExpression = newName
        results.append(newSeries)
    requestContext['totalStack'][stackName] = totalStack
    returnValue(results)


@inlineCallbacks
def areaBetween(requestContext, seriesList):
    """
    Draws the vertical area in between the two series in seriesList. Useful for
    visualizing a range such as the minimum and maximum latency for a service.

    areaBetween expects **exactly one argument** that results in exactly two series
    (see example below). The order of the lower and higher values series does not
    matter. The visualization only works when used in conjunction with
    ``areaMode=stacked``.

    Most likely use case is to provide a band within which another metric should
    move. In such case applying an ``alpha()``, as in the second example, gives
    best visual results.

    Example:

    .. code-block:: none

      &target=areaBetween(service.latency.{min,max})&areaMode=stacked

      &target=alpha(areaBetween(service.latency.{min,max}),0.3)&areaMode=stacked

    If for instance, you need to build a seriesList, you should use the ``group``
    function, like so:

    .. code-block:: none

      &target=areaBetween(group(minSeries(a.*.min),maxSeries(a.*.max)))
    """
    yield defer.succeed(None)
    assert len(
        seriesList) == 2, "areaBetween series argument must reference *exactly* 2 series"
    lower = seriesList[0]
    upper = seriesList[1]

    lower.options['stacked'] = True
    lower.options['invisible'] = True

    upper.options['stacked'] = True
    lower.name = upper.name = "areaBetween(%s)" % upper.pathExpression
    returnValue(seriesList)


@inlineCallbacks
def aliasSub(requestContext, seriesList, search, replace):
    """
    Runs series names through a regex search/replace.

    .. code-block:: none

      &target=aliasSub(ip.*TCP*,"^.*TCP(\d+)","\\1")
    """
    yield defer.succeed(None)
    try:
        seriesList.name = re.sub(search, replace, seriesList.name)
    except AttributeError:
        for series in seriesList:
            series.name = re.sub(search, replace, series.name)
    returnValue(seriesList)


def alias(requestContext, seriesList, newName):
    """
    Takes one metric or a wildcard seriesList and a string in quotes.
    Prints the string instead of the metric name in the legend.

    .. code-block:: none

      &target=alias(Sales.widgets.largeBlue,"Large Blue Widgets")

    """
    try:
        seriesList.name = newName
    except AttributeError:
        for series in seriesList:
            series.name = newName
    return seriesList


@inlineCallbacks
def cactiStyle(requestContext, seriesList, system=None):
    """
    Takes a series list and modifies the aliases to provide column aligned
    output with Current, Max, and Min values in the style of cacti. Optonally
    takes a "system" value to apply unit formatting in the same style as the
    Y-axis.

    .. code-block:: none

      &target=cactiStyle(ganglia.*.net.bytes_out,"si")

    A possible value for ``system`` is ``si``, which would express your values in
    multiples of a thousand. A second option is to use ``binary`` which will
    instead express your values in multiples of 1024 (useful for network devices).

    Column alignment of the Current, Max, Min values works under two conditions:
    you use a monospace font such as terminus and use a single cactiStyle call, as
    separate cactiStyle calls are not aware of each other. In case you have
    different targets for which you would like to have cactiStyle to line up, you
    can use ``group()`` to combine them before applying cactiStyle, such as:

    .. code-block:: none

      &target=cactiStyle(group(metricA,metricB))

    """
    yield defer.succeed(None)
    if 0 == len(seriesList):
        returnValue(seriesList)
    if system:
        fmt = lambda x: "%.2f%s" % format_units(x, system=system)
    else:
        fmt = lambda x: "%.2f" % x
    nameLen = max([0] + [len(getattr(series, "name"))
                         for series in seriesList])
    lastLen = max([0] + [len(fmt(int(safeLast(series) or 3)))
                         for series in seriesList]) + 3
    maxLen = max([0] + [len(fmt(int(safeMax(series) or 3)))
                        for series in seriesList]) + 3
    minLen = max([0] + [len(fmt(int(safeMin(series) or 3)))
                        for series in seriesList]) + 3
    for series in seriesList:
        last = safeLast(series)
        maximum = safeMax(series)
        minimum = safeMin(series)
        if last is None:
            last = NAN
        else:
            last = fmt(float(last))

        if maximum is None:
            maximum = NAN
        else:
            maximum = fmt(float(maximum))
        if minimum is None:
            minimum = NAN
        else:
            minimum = fmt(float(minimum))

        series.name = "%*s Current:%*s Max:%*s Min:%*s " % \
            (-nameLen, series.name,
             -lastLen, last,
             -maxLen, maximum,
             -minLen, minimum)
    returnValue(seriesList)


@inlineCallbacks
def aliasByNode(requestContext, seriesList, *nodes):
    """
    Takes a seriesList and applies an alias derived from one or more "node"
    portion/s of the target name. Node indices are 0 indexed.

    .. code-block:: none

      &target=aliasByNode(ganglia.*.cpu.load5,1)

    """
    yield defer.succeed(None)
    if isinstance(nodes, int):
        nodes = [nodes]
    for series in seriesList:
        metric_pieces = re.search(
            '(?:.*\()?(?P<name>[-\w*\.]+)(?:,|\)?.*)?',
            series.name).groups()[0].split('.')
        series.name = '.'.join(metric_pieces[n] for n in nodes)
    returnValue(seriesList)


@inlineCallbacks
def aliasByMetric(requestContext, seriesList):
    """
    Takes a seriesList and applies an alias derived from the base metric name.

    .. code-block:: none

      &target=aliasByMetric(carbon.agents.graphite.creates)

    """
    yield defer.succeed(None)
    for series in seriesList:
        series.name = series.name.split('.')[-1].split(',')[0]
    returnValue(seriesList)


@inlineCallbacks
def legendValue(requestContext, seriesList, *valueTypes):
    """
    Takes one metric or a wildcard seriesList and a string in quotes.
    Appends a value to the metric name in the legend.  Currently one or several of: `last`, `avg`,
    `total`, `min`, `max`.
    The last argument can be `si` (default) or `binary`, in that case values will be formatted in the
    corresponding system.

    .. code-block:: none

    &target=legendValue(Sales.widgets.largeBlue, 'avg', 'max', 'si')

    """
    yield defer.succeed(None)

    def last(s):
        "Work-around for the missing last point"
        v = s[-1]
        if v is None:
            return s[-2]
        return v

    valueFuncs = {
        'avg': lambda s: safeDiv(safeSum(s), safeLen(s)),
        'total': safeSum,
        'min': safeMin,
        'max': safeMax,
        'last': last
    }
    system = None
    if valueTypes[-1] in ('si', 'binary'):
        system = valueTypes[-1]
        valueTypes = valueTypes[:-1]
    for valueType in valueTypes:
        valueFunc = valueFuncs.get(valueType, lambda s: '(?)')
        if system is None:
            for series in seriesList:
                series.name += " (%s: %s)" % (valueType, valueFunc(series))
        else:
            for series in seriesList:
                value = valueFunc(series)
                formatted = None
                if value is not None:
                    formatted = "%.2f%s" % format_units(value, system=system)
                series.name = "%-20s%-5s%-10s" % (series.name,
                                                  valueType,
                                                  formatted)
    returnValue(seriesList)


@inlineCallbacks
def alpha(requestContext, seriesList, alpha):
    """
    Assigns the given alpha transparency setting to the series. Takes a float value between 0 and 1.
    """
    yield defer.succeed(None)
    for series in seriesList:
        series.options['alpha'] = alpha
    returnValue(seriesList)


@inlineCallbacks
def color(requestContext, seriesList, theColor):
    """
    Assigns the given color to the seriesList

    Example:

    .. code-block:: none

      &target=color(collectd.hostname.cpu.0.user, 'green')
      &target=color(collectd.hostname.cpu.0.system, 'ff0000')
      &target=color(collectd.hostname.cpu.0.idle, 'gray')
      &target=color(collectd.hostname.cpu.0.idle, '6464ffaa')

    """
    yield defer.succeed(None)
    for series in seriesList:
        series.color = theColor
    returnValue(seriesList)


@inlineCallbacks
def substr(requestContext, seriesList, start=0, stop=0):
    """
    Takes one metric or a wildcard seriesList followed by 1 or 2 integers.  Assume that the
    metric name is a list or array, with each element separated by dots.  Prints
    n - length elements of the array (if only one integer n is passed) or n - m
    elements of the array (if two integers n and m are passed).  The list starts
    with element 0 and ends with element (length - 1).

    Example:

    .. code-block:: none

      &target=substr(carbon.agents.hostname.avgUpdateTime,2,4)

    The label would be printed as "hostname.avgUpdateTime".

    """
    yield defer.succeed(None)
    for series in seriesList:
        left = series.name.rfind('(') + 1
        right = series.name.find(')')
        if right < 0:
            right = len(series.name) + 1
        cleanName = series.name[left:right:]
        if int(stop) == 0:
            series.name = '.'.join(cleanName.split('.')[int(start)::])
        else:
            series.name = '.'.join(cleanName.split('.')[int(start):int(stop):])

        # substr(func(a.b,'c'),1) becomes b instead of b,'c'
        series.name = re.sub(',.*$', '', series.name)
    returnValue(seriesList)


@inlineCallbacks
def logarithm(requestContext, seriesList, base=10):
    """
    Takes one metric or a wildcard seriesList, a base, and draws the y-axis in logarithmic
    format.  If base is omitted, the function defaults to base 10.

    Example:

    .. code-block:: none

      &target=log(carbon.agents.hostname.avgUpdateTime,2)

    """
    yield defer.succeed(None)
    results = []
    for series in seriesList:
        newValues = []
        for val in series:
            if val is None:
                newValues.append(None)
            elif val <= 0:
                newValues.append(None)
            else:
                newValues.append(math.log(val, base))
        newName = "log(%s, %s)" % (series.name, base)
        newSeries = TimeSeries(
            newName,
            series.start,
            series.end,
            series.step,
            newValues)
        newSeries.pathExpression = newName
        results.append(newSeries)
    returnValue(results)


@inlineCallbacks
def maximumAbove(requestContext, seriesList, n):
    """
    Takes one metric or a wildcard seriesList followed by a constant n.
    Draws only the metrics with a maximum value above n.

    Example:

    .. code-block:: none

      &target=maximumAbove(system.interface.eth*.packetsSent,1000)

    This would only display interfaces which sent more than 1000 packets/min.
    """
    yield defer.succeed(None)
    results = []
    for series in seriesList:
        if max(series) > n:
            results.append(series)
    returnValue(results)


@inlineCallbacks
def minimumAbove(requestContext, seriesList, n):
    """
    Takes one metric or a wildcard seriesList followed by a constant n.
    Draws only the metrics with a minimum value above n.

    Example:

    .. code-block:: none

      &target=minimumAbove(system.interface.eth*.packetsSent,1000)

    This would only display interfaces which sent more than 1000 packets/min.
    """
    yield defer.succeed(None)
    results = []
    for series in seriesList:
        if min(series) > n:
            results.append(series)
    returnValue(results)


@inlineCallbacks
def maximumBelow(requestContext, seriesList, n):
    """
    Takes one metric or a wildcard seriesList followed by a constant n.
    Draws only the metrics with a maximum value below n.

    Example:

    .. code-block:: none

      &target=maximumBelow(system.interface.eth*.packetsSent,1000)

    This would only display interfaces which sent less than 1000 packets/min.
    """

    yield defer.succeed(None)
    result = []
    for series in seriesList:
        if max(series) <= n:
            result.append(series)
    returnValue(result)


@inlineCallbacks
def minimumBelow(requestContext, seriesList, n):
    """
    Takes one metric or a wildcard seriesList followed by a constant n.
    Draws only the metrics with a minimum value below n.

    Example:

    .. code-block:: none

      &target=minimumBelow(system.interface.eth*.packetsSent,1000)

    This would only display interfaces which at one point sent less than 1000 packets/min.
    """

    yield defer.succeed(None)
    result = []
    for series in seriesList:
        if min(series) <= n:
            result.append(series)
    returnValue(result)


@inlineCallbacks
def highestCurrent(requestContext, seriesList, n):
    """
    Takes one metric or a wildcard seriesList followed by an integer N.
    Out of all metrics passed, draws only the N metrics with the highest value
    at the end of the time period specified.

    Example:

    .. code-block:: none

      &target=highestCurrent(server*.instance*.threads.busy,5)

    Draws the 5 servers with the highest busy threads.

    """
    yield defer.succeed(None)
    returnValue(sorted(seriesList, key=safeLast)[-n:])


@inlineCallbacks
def highestMax(requestContext, seriesList, n):
    """
    Takes one metric or a wildcard seriesList followed by an integer N.

    Out of all metrics passed, draws only the N metrics with the highest maximum
    value in the time period specified.

    Example:

    .. code-block:: none

      &target=highestMax(server*.instance*.threads.busy,5)

    Draws the top 5 servers who have had the most busy threads during the time
    period specified.

    """
    yield defer.succeed(None)
    result_list = sorted(seriesList, key=lambda s: max(s))[-n:]

    returnValue(sorted(result_list, key=lambda s: max(s), reverse=True))


@inlineCallbacks
def lowestCurrent(requestContext, seriesList, n):
    """
    Takes one metric or a wildcard seriesList followed by an integer N.
    Out of all metrics passed, draws only the N metrics with the lowest value at
    the end of the time period specified.

    Example:

    .. code-block:: none

      &target=lowestCurrent(server*.instance*.threads.busy,5)

    Draws the 5 servers with the least busy threads right now.

    """

    yield defer.succeed(None)
    returnValue(sorted(seriesList, key=safeLast)[:n])


@inlineCallbacks
def currentAbove(requestContext, seriesList, n):
    """
    Takes one metric or a wildcard seriesList followed by an integer N.
    Out of all metrics passed, draws only the  metrics whose value is above N
    at the end of the time period specified.

    Example:

    .. code-block:: none

      &target=currentAbove(server*.instance*.threads.busy,50)

    Draws the servers with more than 50 busy threads.

    """
    yield defer.succeed(None)
    returnValue([series for series in seriesList if safeLast(series) >= n])


@inlineCallbacks
def currentBelow(requestContext, seriesList, n):
    """
    Takes one metric or a wildcard seriesList followed by an integer N.
    Out of all metrics passed, draws only the  metrics whose value is below N
    at the end of the time period specified.

    Example:

    .. code-block:: none

      &target=currentBelow(server*.instance*.threads.busy,3)

    Draws the servers with less than 3 busy threads.

    """
    yield defer.succeed(None)
    returnValue([series for series in seriesList if safeLast(series) <= n])


@inlineCallbacks
def highestAverage(requestContext, seriesList, n):
    """
    Takes one metric or a wildcard seriesList followed by an integer N.
    Out of all metrics passed, draws only the top N metrics with the highest
    average value for the time period specified.

    Example:

    .. code-block:: none

      &target=highestAverage(server*.instance*.threads.busy,5)

    Draws the top 5 servers with the highest average value.

    """

    yield defer.succeed(None)
    returnValue(
        sorted(seriesList, key=lambda s: safeDiv(safeSum(s), safeLen(s)))[-n:])


@inlineCallbacks
def lowestAverage(requestContext, seriesList, n):
    """
    Takes one metric or a wildcard seriesList followed by an integer N.
    Out of all metrics passed, draws only the bottom N metrics with the lowest
    average value for the time period specified.

    Example:

    .. code-block:: none

      &target=lowestAverage(server*.instance*.threads.busy,5)

    Draws the bottom 5 servers with the lowest average value.

    """

    yield defer.succeed(None)
    returnValue(
        sorted(
            seriesList,
            key=lambda s: safeDiv(
                safeSum(s),
                safeLen(s)))[
            :n])


@inlineCallbacks
def averageAbove(requestContext, seriesList, n):
    """
    Takes one metric or a wildcard seriesList followed by an integer N.
    Out of all metrics passed, draws only the metrics with an average value
    above N for the time period specified.

    Example:

    .. code-block:: none

      &target=averageAbove(server*.instance*.threads.busy,25)

    Draws the servers with average values above 25.

    """
    yield defer.succeed(None)
    returnValue([series for series in seriesList if safeDiv(
        safeSum(series), safeLen(series)) >= n])


@inlineCallbacks
def averageBelow(requestContext, seriesList, n):
    """
    Takes one metric or a wildcard seriesList followed by an integer N.
    Out of all metrics passed, draws only the metrics with an average value
    below N for the time period specified.

    Example:

    .. code-block:: none

      &target=averageBelow(server*.instance*.threads.busy,25)

    Draws the servers with average values below 25.

    """
    yield defer.succeed(None)
    returnValue([series for series in seriesList if safeDiv(
        safeSum(series), safeLen(series)) <= n])


def _getPercentile(points, n, interpolate=False):
    """
    Percentile is calculated using the method outlined in the NIST Engineering
    Statistics Handbook:
    http://www.itl.nist.gov/div898/handbook/prc/section2/prc252.htm
    """
    sortedPoints = sorted([p for p in points if p is not None])
    if len(sortedPoints) == 0:
        return None
    fractionalRank = (n / 100.0) * (len(sortedPoints) + 1)
    rank = int(fractionalRank)
    rankFraction = fractionalRank - rank

    if not interpolate:
        rank += int(math.ceil(rankFraction))

    if rank == 0:
        percentile = sortedPoints[0]
    elif rank - 1 == len(sortedPoints):
        percentile = sortedPoints[-1]
    else:
        percentile = sortedPoints[rank - 1]  # Adjust for 0-index

    if interpolate:
        if rank != len(sortedPoints):  # if a next value exists
            nextValue = sortedPoints[rank]
            percentile = percentile + rankFraction * (nextValue - percentile)

    return percentile


@inlineCallbacks
def nPercentile(requestContext, seriesList, n):
    """Returns n-percent of each series in the seriesList."""
    assert n, 'The requested percent is required to be greater than 0'

    yield defer.succeed(None)
    results = []
    for s in seriesList:
        # Create a sorted copy of the TimeSeries excluding None values in the
        # values list.
        s_copy = TimeSeries(s.name,
                            s.start,
                            s.end,
                            s.step,
                            sorted([item for item in s if item is not None]))
        if not s_copy:
            continue  # Skip this series because it is empty.

        perc_val = _getPercentile(s_copy, n)
        if perc_val is not None:
            name = 'nPercentile(%s, %g)' % (s_copy.name, n)
            point_count = int((s.end - s.start) / s.step)
            perc_series = TimeSeries(
                name,
                s_copy.start,
                s_copy.end,
                s_copy.step,
                [perc_val] *
                point_count)
            perc_series.pathExpression = name
            results.append(perc_series)
    returnValue(results)


@inlineCallbacks
def averageOutsidePercentile(requestContext, seriesList, n):
    """
    Removes functions lying inside an average percentile interval
    """
    yield defer.succeed(None)
    averages = []

    for s in seriesList:
        averages.append(safeDiv(safeSum(s), safeLen(s)))

    if n < 50:
        n = 100 - n

    lowPercentile = _getPercentile(averages, 100 - n)
    highPercentile = _getPercentile(averages, n)

    returnValue([s for s in seriesList if not lowPercentile < safeDiv(
        safeSum(s), safeLen(s)) < highPercentile])


@inlineCallbacks
def removeBetweenPercentile(requestContext, seriesList, n):
    """
    Removes lines who do not have an value lying in the x-percentile of all the values at a moment
    """
    yield defer.succeed(None)
    if n < 50:
        n = 100 - n

    transposed = zip(*seriesList)

    lowPercentiles = [_getPercentile(col, 100 - n) for col in transposed]
    highPercentiles = [_getPercentile(col, n) for col in transposed]

    returnValue([l for l in seriesList if sum([not lowPercentiles[
                val_i] < val < highPercentiles[val_i] for (val_i, val) in enumerate(l)]) > 0])


@inlineCallbacks
def removeAbovePercentile(requestContext, seriesList, n):
    """
    Removes data above the nth percentile from the series or list of series provided.
    Values above this percentile are assigned a value of None.
    """
    yield defer.succeed(None)
    for s in seriesList:
        s.name = 'removeAbovePercentile(%s, %d)' % (s.name, n)
        s.pathExpression = s.name
        percentile = nPercentile(requestContext, [s], n)[0][0]
        for (index, val) in enumerate(s):
            if val > percentile:
                s[index] = None

    returnValue(seriesList)


@inlineCallbacks
def removeAboveValue(requestContext, seriesList, n):
    """
    Removes data above the given threshold from the series or list of series provided.
    Values above this threshold are assigned a value of None.
    """
    yield defer.succeed(None)
    for s in seriesList:
        s.name = 'removeAboveValue(%s, %d)' % (s.name, n)
        s.pathExpression = s.name
        for (index, val) in enumerate(s):
            if val > n:
                s[index] = None

    returnValue(seriesList)


@inlineCallbacks
def removeBelowPercentile(requestContext, seriesList, n):
    """
    Removes data below the nth percentile from the series or list of series provided.
    Values below this percentile are assigned a value of None.
    """
    yield defer.succeed(None)
    for s in seriesList:
        s.name = 'removeBelowPercentile(%s, %d)' % (s.name, n)
        s.pathExpression = s.name
        percentile = nPercentile(requestContext, [s], n)[0][0]
        for (index, val) in enumerate(s):
            if val < percentile:
                s[index] = None

    returnValue(seriesList)


@inlineCallbacks
def removeBelowValue(requestContext, seriesList, n):
    """
    Removes data below the given threshold from the series or list of series provided.
    Values below this threshold are assigned a value of None.
    """
    yield defer.succeed(None)
    for s in seriesList:
        s.name = 'removeBelowValue(%s, %d)' % (s.name, n)
        s.pathExpression = s.name
        for (index, val) in enumerate(s):
            if val < n:
                s[index] = None

    returnValue(seriesList)


@inlineCallbacks
def limit(requestContext, seriesList, n):
    """
    Takes one metric or a wildcard seriesList followed by an integer N.

    Only draw the first N metrics.  Useful when testing a wildcard in a metric.

    Example:

    .. code-block:: none

      &target=limit(server*.instance*.memory.free,5)

    Draws only the first 5 instance's memory free.

    """
    yield defer.succeed(None)
    returnValue(seriesList[0:n])


@inlineCallbacks
def sortByName(requestContext, seriesList):
    """
    Takes one metric or a wildcard seriesList.

    Sorts the list of metrics by the metric name.
    """
    yield defer.succeed(None)

    def compare(x, y):
        return cmp(x.name, y.name)

    seriesList.sort(compare)
    returnValue(seriesList)


@inlineCallbacks
def sortByTotal(requestContext, seriesList):
    """
    Takes one metric or a wildcard seriesList.

    Sorts the list of metrics by the sum of values across the time period
    specified.
    """
    yield defer.succeed(None)

    def compare(x, y):
        return cmp(safeSum(y), safeSum(x))

    seriesList.sort(compare)
    returnValue(seriesList)


@inlineCallbacks
def sortByMaxima(requestContext, seriesList):
    """
    Takes one metric or a wildcard seriesList.

    Sorts the list of metrics by the maximum value across the time period
    specified.  Useful with the &areaMode=all parameter, to keep the
    lowest value lines visible.

    Example:

    .. code-block:: none

      &target=sortByMaxima(server*.instance*.memory.free)

    """
    yield defer.succeed(None)

    def compare(x, y):
        return cmp(max(y), max(x))
    seriesList.sort(compare)
    returnValue(seriesList)


@inlineCallbacks
def sortByMinima(requestContext, seriesList):
    """
    Takes one metric or a wildcard seriesList.

    Sorts the list of metrics by the lowest value across the time period
    specified.

    Example:

    .. code-block:: none

      &target=sortByMinima(server*.instance*.memory.free)

    """
    yield defer.succeed(None)

    def compare(x, y):
        return cmp(min(x), min(y))
    newSeries = [series for series in seriesList if max(series) > 0]
    newSeries.sort(compare)
    returnValue(newSeries)


@inlineCallbacks
def useSeriesAbove(requestContext, seriesList, value, search, replace):
    """
    Compares the maximum of each series against the given `value`. If the series
    maximum is greater than `value`, the regular expression search and replace is
    applied against the series name to plot a related metric

    e.g. given useSeriesAbove(ganglia.metric1.reqs,10,'reqs','time'),
    the response time metric will be plotted only when the maximum value of the
    corresponding request/s metric is > 10

    .. code-block:: none

      &target=useSeriesAbove(ganglia.metric1.reqs,10,"reqs","time")
    """
    yield defer.succeed(None)
    newSeries = []

    for series in seriesList:
        newname = re.sub(search, replace, series.name)
        if max(series) > value:
            n = yield evaluateTarget(requestContext, newname)
            if n is not None and len(n) > 0:
                newSeries.append(n[0])

    returnValue(newSeries)


@inlineCallbacks
def fallbackSeries(requestContext, seriesList, fallback):
    """
    Takes a wildcard seriesList, and a second fallback metric.
    If the wildcard does not match any series, draws the fallback metric.

    Example:

    .. code-block:: none

      &target=fallbackSeries(server*.requests_per_second, constantLine(0))

    Draws a 0 line when server metric does not exist.

    """
    yield defer.succeed(None)
    if len(seriesList) > 0:
        returnValue(seriesList)
    else:
        returnValue(fallback)


@inlineCallbacks
def mostDeviant(requestContext, seriesList, n):
    """
    Takes one metric or a wildcard seriesList followed by an integer N.
    Draws the N most deviant metrics.
    To find the deviants, the standard deviation (sigma) of each series
    is taken and ranked. The top N standard deviations are returned.

      Example:

    .. code-block:: none

      &target=mostDeviant(server*.instance*.memory.free, 5)

    Draws the 5 instances furthest from the average memory free.

    """

    yield defer.succeed(None)
    deviants = []
    for series in seriesList:
        mean = safeDiv(safeSum(series), safeLen(series))
        if mean is None:
            continue
        square_sum = sum(
            [(value - mean) ** 2 for value in series if value is not None])
        sigma = safeDiv(square_sum, safeLen(series))
        if sigma is None:
            continue
        deviants.append((sigma, series))
    deviants.sort(key=lambda i: i[0], reverse=True)  # sort by sigma
    # return the n most deviant series
    returnValue([series for (_, series) in deviants][:n])


@inlineCallbacks
def stdev(requestContext, seriesList, points, windowTolerance=0.1):
    """
    Takes one metric or a wildcard seriesList followed by an integer N.
    Draw the Standard Deviation of all metrics passed for the past N datapoints.
    If the ratio of null points in the window is greater than windowTolerance,
    skip the calculation. The default for windowTolerance is 0.1 (up to 10% of points
    in the window can be missing). Note that if this is set to 0.0, it will cause large
    gaps in the output anywhere a single point is missing.

    Example:

    .. code-block:: none

      &target=stdev(server*.instance*.threads.busy,30)
      &target=stdev(server*.instance*.cpu.system,30,0.0)

    """
    yield defer.succeed(None)

    # For this we take the standard deviation in terms of the moving average
    # and the moving average of series squares.
    for (seriesIndex, series) in enumerate(seriesList):
        stddevSeries = TimeSeries(
            "stddev(%s,%d)" %
            (series.name,
             int(points)),
            series.start,
            series.end,
            series.step,
            [])
        stddevSeries.pathExpression = "stddev(%s,%d)" % (
            series.name, int(points))

        validPoints = 0
        currentSum = 0
        currentSumOfSquares = 0
        for (index, newValue) in enumerate(series):
            # Mark whether we've reached our window size - dont drop points out
            # otherwise
            if index < points:
                bootstrapping = True
                droppedValue = None
            else:
                bootstrapping = False
                droppedValue = series[index - points]

            # Track non-None points in window
            if not bootstrapping and droppedValue is not None:
                validPoints -= 1
            if newValue is not None:
                validPoints += 1

            # Remove the value that just dropped out of the window
            if not bootstrapping and droppedValue is not None:
                currentSum -= droppedValue
                currentSumOfSquares -= droppedValue ** 2

            # Add in the value that just popped in the window
            if newValue is not None:
                currentSum += newValue
                currentSumOfSquares += newValue ** 2

            if validPoints > 0 and \
                    float(validPoints) / points >= windowTolerance:

                try:
                    deviation = math.sqrt(
                        validPoints * currentSumOfSquares - currentSum ** 2) / validPoints
                except ValueError:
                    deviation = None
                stddevSeries.append(deviation)
            else:
                stddevSeries.append(None)

        seriesList[seriesIndex] = stddevSeries

    returnValue(seriesList)


@inlineCallbacks
def secondYAxis(requestContext, seriesList):
    """
    Graph the series on the secondary Y axis.
    """
    yield defer.succeed(None)
    for series in seriesList:
        series.options['secondYAxis'] = True
        series.name = 'secondYAxis(%s)' % series.name
    returnValue(seriesList)


@inlineCallbacks
def _fetchWithBootstrap(requestContext, seriesList, **delta_kwargs):
    'Request the same data but with a bootstrap period at the beginning'
    bootstrapContext = requestContext.copy()
    bootstrapContext['startTime'] = requestContext[
        'startTime'] - timedelta(**delta_kwargs)
    bootstrapContext['endTime'] = requestContext['startTime']

    bootstrapList = []
    for series in seriesList:
        if series.pathExpression in [b.pathExpression for b in bootstrapList]:
            # This pathExpression returns multiple series and we already
            # fetched it
            continue
        bootstraps = yield evaluateTarget(bootstrapContext, series.pathExpression)
        bootstrapList.extend(bootstraps)

    newSeriesList = []
    for bootstrap, original in zip(bootstrapList, seriesList):
        newValues = []
        if bootstrap.step != original.step:
            ratio = bootstrap.step / original.step
            for value in bootstrap:
                # XXX For series with aggregationMethod = sum this should also
                # divide by the ratio to bring counts to the same time unit
                # ...but we have no way of knowing whether that's the case
                newValues.extend([value] * ratio)
        else:
            newValues.extend(bootstrap)
        newValues.extend(original)

        newSeries = TimeSeries(
            original.name,
            bootstrap.start,
            original.end,
            original.step,
            newValues)
        newSeries.pathExpression = series.pathExpression
        newSeriesList.append(newSeries)

    returnValue(newSeriesList)


def _trimBootstrap(bootstrap, original):
    'Trim the bootstrap period off the front of this series so it matches the original'
    original_len = len(original)
    length_limit = (original_len * original.step) / bootstrap.step
    trim_start = bootstrap.end - (length_limit * bootstrap.step)
    trimmed = TimeSeries(bootstrap.name,
                         trim_start,
                         bootstrap.end,
                         bootstrap.step,
                         bootstrap[-length_limit:])
    return trimmed


def holtWintersIntercept(
        alpha,
        actual,
        last_season,
        last_intercept,
        last_slope):
    return alpha * (actual - last_season) \
        + (1 - alpha) * (last_intercept + last_slope)


def holtWintersSlope(beta, intercept, last_intercept, last_slope):
    return beta * (intercept - last_intercept) + (1 - beta) * last_slope


def holtWintersSeasonal(gamma, actual, intercept, last_season):
    return gamma * (actual - intercept) + (1 - gamma) * last_season


def holtWintersDeviation(gamma, actual, prediction, last_seasonal_dev):
    if prediction is None:
        prediction = 0
    return gamma * math.fabs(actual - prediction) + \
        (1 - gamma) * last_seasonal_dev


def holtWintersAnalysis(series):
    alpha = gamma = 0.1
    beta = 0.0035
    # season is currently one day
    season_length = (24 * 60 * 60) / series.step
    intercept = 0
    slope = 0
    intercepts = list()
    slopes = list()
    seasonals = list()
    predictions = list()
    deviations = list()

    def getLastSeasonal(i):
        j = i - season_length
        if j >= 0:
            return seasonals[j]
        return 0

    def getLastDeviation(i):
        j = i - season_length
        if j >= 0:
            return deviations[j]
        return 0

    last_seasonal = 0
    last_seasonal_dev = 0
    next_last_seasonal = 0
    next_pred = None

    for i, actual in enumerate(series):
        if actual is None:
            # missing input values break all the math
            # do the best we can and move on
            intercepts.append(None)
            slopes.append(0)
            seasonals.append(0)
            predictions.append(next_pred)
            deviations.append(0)
            next_pred = None
            continue

        if i == 0:
            last_intercept = actual
            last_slope = 0
            # seed the first prediction as the first actual
            prediction = actual
        else:
            last_intercept = intercepts[-1]
            last_slope = slopes[-1]
            if last_intercept is None:
                last_intercept = actual
            prediction = next_pred

        last_seasonal = getLastSeasonal(i)
        next_last_seasonal = getLastSeasonal(i + 1)
        last_seasonal_dev = getLastDeviation(i)

        intercept = holtWintersIntercept(
            alpha,
            actual,
            last_seasonal,
            last_intercept,
            last_slope)
        slope = holtWintersSlope(beta, intercept, last_intercept, last_slope)
        seasonal = holtWintersSeasonal(gamma, actual, intercept, last_seasonal)
        next_pred = intercept + slope + next_last_seasonal
        deviation = holtWintersDeviation(
            gamma,
            actual,
            prediction,
            last_seasonal_dev)

        intercepts.append(intercept)
        slopes.append(slope)
        seasonals.append(seasonal)
        predictions.append(prediction)
        deviations.append(deviation)

    # make the new forecast series
    forecastName = "holtWintersForecast(%s)" % series.name
    forecastSeries = TimeSeries(
        forecastName,
        series.start,
        series.end,
        series.step,
        predictions)
    forecastSeries.pathExpression = forecastName

    # make the new deviation series
    deviationName = "holtWintersDeviation(%s)" % series.name
    deviationSeries = TimeSeries(
        deviationName,
        series.start,
        series.end,
        series.step,
        deviations)
    deviationSeries.pathExpression = deviationName

    results = {
        'predictions': forecastSeries,
        'deviations': deviationSeries,
        'intercepts': intercepts,
        'slopes': slopes,
        'seasonals': seasonals}
    return results


@inlineCallbacks
def holtWintersForecast(requestContext, seriesList):
    """
    Performs a Holt-Winters forecast using the series as input data. Data from
    one week previous to the series is used to bootstrap the initial forecast.
    """
    yield defer.succeed(None)
    results = []
    bootstrapList = yield _fetchWithBootstrap(requestContext, seriesList, days=7)
    for bootstrap, series in zip(bootstrapList, seriesList):
        analysis = holtWintersAnalysis(bootstrap)
        results.append(_trimBootstrap(analysis['predictions'], series))
    returnValue(results)


@inlineCallbacks
def holtWintersConfidenceBands(requestContext, seriesList, delta=3):
    """
    Performs a Holt-Winters forecast using the series as input data and plots
    upper and lower bands with the predicted forecast deviations.
    """
    yield defer.succeed(None)
    results = []
    bootstrapList = yield _fetchWithBootstrap(requestContext, seriesList, days=7)
    for bootstrap, series in zip(bootstrapList, seriesList):
        analysis = holtWintersAnalysis(bootstrap)
        forecast = _trimBootstrap(analysis['predictions'], series)
        deviation = _trimBootstrap(analysis['deviations'], series)
        seriesLength = len(forecast)
        i = 0
        upperBand = list()
        lowerBand = list()
        while i < seriesLength:
            forecast_item = forecast[i]
            deviation_item = deviation[i]
            i = i + 1
            if forecast_item is None or deviation_item is None:
                upperBand.append(None)
                lowerBand.append(None)
            else:
                scaled_deviation = delta * deviation_item
                upperBand.append(forecast_item + scaled_deviation)
                lowerBand.append(forecast_item - scaled_deviation)

        upperName = "holtWintersConfidenceUpper(%s)" % series.name
        lowerName = "holtWintersConfidenceLower(%s)" % series.name
        upperSeries = TimeSeries(
            upperName,
            forecast.start,
            forecast.end,
            forecast.step,
            upperBand)
        lowerSeries = TimeSeries(
            lowerName,
            forecast.start,
            forecast.end,
            forecast.step,
            lowerBand)
        upperSeries.pathExpression = series.pathExpression
        lowerSeries.pathExpression = series.pathExpression
        results.append(lowerSeries)
        results.append(upperSeries)
    returnValue(results)


@inlineCallbacks
def holtWintersAberration(requestContext, seriesList, delta=3):
    """
    Performs a Holt-Winters forecast using the series as input data and plots the
    positive or negative deviation of the series data from the forecast.
    """
    yield defer.succeed(None)
    results = []
    for series in seriesList:
        confidenceBands = holtWintersConfidenceBands(
            requestContext,
            [series],
            delta)
        lowerBand = confidenceBands[0]
        upperBand = confidenceBands[1]
        aberration = list()
        for i, actual in enumerate(series):
            if series[i] is None:
                aberration.append(0)
            elif upperBand[i] is not None and series[i] > upperBand[i]:
                aberration.append(series[i] - upperBand[i])
            elif lowerBand[i] is not None and series[i] < lowerBand[i]:
                aberration.append(series[i] - lowerBand[i])
            else:
                aberration.append(0)

        newName = "holtWintersAberration(%s)" % series.name
        results.append(
            TimeSeries(
                newName,
                series.start,
                series.end,
                series.step,
                aberration))
    returnValue(results)


@inlineCallbacks
def holtWintersConfidenceArea(requestContext, seriesList, delta=3):
    """
    Performs a Holt-Winters forecast using the series as input data and plots the
    area between the upper and lower bands of the predicted forecast deviations.
    """
    yield defer.succeed(None)
    bands = holtWintersConfidenceBands(requestContext, seriesList, delta)
    results = areaBetween(requestContext, bands)
    for series in results:
        series.name = series.name.replace(
            'areaBetween',
            'holtWintersConfidenceArea')
    returnValue(results)


@inlineCallbacks
def drawAsInfinite(requestContext, seriesList):
    """
    Takes one metric or a wildcard seriesList.
    If the value is zero, draw the line at 0.  If the value is above zero, draw
    the line at infinity. If the value is null or less than zero, do not draw
    the line.

    Useful for displaying on/off metrics, such as exit codes. (0 = success,
    anything else = failure.)

    Example:

    .. code-block:: none

      drawAsInfinite(Testing.script.exitCode)

    """
    yield defer.succeed(None)
    for series in seriesList:
        series.options['drawAsInfinite'] = True
        series.name = 'drawAsInfinite(%s)' % series.name
    returnValue(seriesList)


@inlineCallbacks
def lineWidth(requestContext, seriesList, width):
    """
    Takes one metric or a wildcard seriesList, followed by a float F.

    Draw the selected metrics with a line width of F, overriding the default
    value of 1, or the &lineWidth=X.X parameter.

    Useful for highlighting a single metric out of many, or having multiple
    line widths in one graph.

    Example:

    .. code-block:: none

      &target=lineWidth(server01.instance01.memory.free,5)

    """
    yield defer.succeed(None)
    for series in seriesList:
        series.options['lineWidth'] = width
    returnValue(seriesList)


@inlineCallbacks
def dashed(requestContext, *seriesList):
    """
    Takes one metric or a wildcard seriesList, followed by a float F.

    Draw the selected metrics with a dotted line with segments of length F
    If omitted, the default length of the segments is 5.0

    Example:

    .. code-block:: none

      &target=dashed(server01.instance01.memory.free,2.5)

    """

    yield defer.succeed(None)
    if len(seriesList) == 2:
        dashLength = seriesList[1]
    else:
        dashLength = 5
    for series in seriesList[0]:
        series.name = 'dashed(%s, %d)' % (series.name, dashLength)
        series.options['dashed'] = dashLength
    returnValue(seriesList[0])


@inlineCallbacks
def timeStack(
        requestContext,
        seriesList,
        timeShiftUnit,
        timeShiftStart,
        timeShiftEnd):
    """
    Takes one metric or a wildcard seriesList, followed by a quoted string with the
    length of time (See ``from / until`` in the render\_api_ for examples of time formats).
    Also takes a start multiplier and end multiplier for the length of time

    create a seriesList which is composed the original metric series stacked with time shifts
    starting time shifts from the start multiplier through the end multiplier

    Useful for looking at history, or feeding into averageSeries or stddevSeries.

    Example:

    .. code-block:: none

      &target=timeStack(Sales.widgets.largeBlue,"1d",0,7)    # create a series for today and each of the previous 7 days

    """
    yield defer.succeed(None)
    # Default to negative. parseTimeOffset defaults to +
    if timeShiftUnit[0].isdigit():
        timeShiftUnit = '-' + timeShiftUnit
    delta = parseTimeOffset(timeShiftUnit)

    # if len(seriesList) > 1, they will all have the same pathExpression,
    # which is all we care about.
    series = seriesList[0]

    results = []
    timeShiftStartint = int(timeShiftStart)
    timeShiftEndint = int(timeShiftEnd)

    for shft in range(timeShiftStartint, timeShiftEndint):
        myContext = requestContext.copy()
        innerDelta = delta * shft
        myContext['delta'] = abs(innerDelta.seconds + (innerDelta.days * 86400))
        myContext['startTime'] = requestContext['startTime'] + innerDelta
        myContext['endTime'] = requestContext['endTime'] + innerDelta
        for shiftedSeries in (yield evaluateTarget(myContext, series.pathExpression)):
            shiftedSeries.name = 'timeShift(%s, %s, %s)' % (
                shiftedSeries.name, timeShiftUnit, shft)
            shiftedSeries.pathExpression = shiftedSeries.name
            shiftedSeries.start = series.start
            shiftedSeries.end = series.end
            results.append(shiftedSeries)

    returnValue(results)


@inlineCallbacks
def timeShift(requestContext, seriesList, timeShift, resetEnd=True):
    """
    Takes one metric or a wildcard seriesList, followed by a quoted string with the
    length of time (See ``from / until`` in the render\_api_ for examples of time formats).

    Draws the selected metrics shifted in time. If no sign is given, a minus sign ( - ) is
    implied which will shift the metric back in time. If a plus sign ( + ) is given, the
    metric will be shifted forward in time.

    Will reset the end date range automatically to the end of the base stat unless
    resetEnd is False. Example case is when you timeshift to last week and have the graph
    date range set to include a time in the future, will limit this timeshift to pretend
    ending at the current time. If resetEnd is False, will instead draw full range including
    future time.

    Useful for comparing a metric against itself at a past periods or correcting data
    stored at an offset.

    Example:

    .. code-block:: none

      &target=timeShift(Sales.widgets.largeBlue,"7d")
      &target=timeShift(Sales.widgets.largeBlue,"-7d")
      &target=timeShift(Sales.widgets.largeBlue,"+1h")

    """
    yield defer.succeed(None)
    # Default to negative. parseTimeOffset defaults to +
    if timeShift[0].isdigit():
        timeShift = '-' + timeShift
    delta = parseTimeOffset(timeShift)
    myContext = requestContext.copy()
    myContext['startTime'] = requestContext['startTime'] + delta
    myContext['endTime'] = requestContext['endTime'] + delta
    myContext['delta'] = abs(delta.seconds + (delta.days * 86400))
    results = []
    if len(seriesList) > 0:
        # if len(seriesList) > 1, they will all have the same pathExpression,
        # which is all we care about.
        series = seriesList[0]

        for shiftedSeries in (yield evaluateTarget(myContext, series.pathExpression)):
            shiftedSeries.name = 'timeShift(%s, %s)' % (
                shiftedSeries.name, timeShift)
            if resetEnd:
                shiftedSeries.end = series.end
            else:
                shiftedSeries.end = shiftedSeries.end - \
                    shiftedSeries.start + series.start
            shiftedSeries.start = series.start
            results.append(shiftedSeries)

    returnValue(results)


@inlineCallbacks
def timeSlice(requestContext, seriesList, startSliceAt, endSliceAt="now"):
    """
    Takes one metric or a wildcard metric, followed by a quoted string with the
    time to start the line and another quoted string with the time to end the line.
    The start and end times are inclusive. See ``from / until`` in the render\_api_
    for examples of time formats.

    Useful for filtering out a part of a series of data from a wider range of
    data.

    Example:

    .. code-block:: none

      &target=timeSlice(network.core.port1,"00:00 20140101","11:59 20140630")
      &target=timeSlice(network.core.port1,"12:00 20140630","now")

    """

    yield defer.succeed(None)
    results = []
    start = time.mktime(parseATTime(startSliceAt).timetuple())
    end = time.mktime(parseATTime(endSliceAt).timetuple())

    for slicedSeries in seriesList:
        slicedSeries.name = 'timeSlice(%s, %s, %s)' % (
            slicedSeries.name, int(start), int(end))

        curr = time.mktime(requestContext["startTime"].timetuple())
        for i, v in enumerate(slicedSeries):
            if v is None or curr < start or curr > end:
                slicedSeries[i] = None
            curr += slicedSeries.step

        results.append(slicedSeries)

    returnValue(results)


@inlineCallbacks
def constantLine(requestContext, value):
    """
    Takes a float F.

    Draws a horizontal line at value F across the graph.

    Example:

    .. code-block:: none

      &target=constantLine(123.456)

    """
    yield defer.succeed(None)
    name = "constantLine(%s)" % str(value)
    start = int(epoch(requestContext['startTime']))
    end = int(epoch(requestContext['endTime']))
    step = (end - start) / 1.0
    series = TimeSeries(str(value), start, end, step, [value, value])
    series.pathExpression = name
    returnValue([series])


@inlineCallbacks
def aggregateLine(requestContext, seriesList, func='avg'):
    """
    Draws a horizontal line based the function applied to the series.


    Note: By default, the graphite renderer consolidates data points by
    averaging data points over time. If you are using the 'min' or 'max'
    function for aggregateLine, this can cause an unusual gap in the
    line drawn by this function and the data itself. To fix this, you
    should use the consolidateBy() function with the same function
    argument you are using for aggregateLine. This will ensure that the
    proper data points are retained and the graph should line up
    correctly.

    Example:

    .. code-block:: none

      &target=aggregateLine(server.connections.total, 'avg')

    """
    yield defer.succeed(None)
    t_funcs = {'avg': safeAvg, 'min': safeMin, 'max': safeMax}

    if func not in t_funcs:
        raise ValueError("Invalid function %s" % func)

    value = t_funcs[func](seriesList[0])
    name = 'aggregateLine(%s,%d)' % (seriesList[0].pathExpression, value)

    series = constantLine(requestContext, value)[0]
    series.name = name

    returnValue([series])


@inlineCallbacks
def threshold(requestContext, value, label=None, color=None):
    """
    Takes a float F, followed by a label (in double quotes) and a color.
    (See ``bgcolor`` in the render\_api_ for valid color names & formats.)

    Draws a horizontal line at value F across the graph.

    Example:

    .. code-block:: none

      &target=threshold(123.456, "omgwtfbbq", red)

    """

    yield defer.succeed(None)
    series = constantLine(requestContext, value)[0]
    if label:
        series.name = label
    if color:
        series.color = color

    returnValue([series])


@inlineCallbacks
def transformNull(requestContext, seriesList, default=0):
    """
    Takes a metric or wild card seriesList and an optional value
    to transform Nulls to. Default is 0. This method compliments
    drawNullAsZero flag in graphical mode but also works in text only
    mode.
    Example:

    .. code-block:: none

      &target=transformNull(webapp.pages.*.views,-1)

    This would take any page that didn't have values and supply negative 1 as a default.
    Any other numeric value may be used as well.
    """
    yield defer.succeed(None)

    def transform(v):
        if v is None:
            return default
        else:
            return v

    for series in seriesList:
        series.name = "transformNull(%s,%g)" % (series.name, default)
        series.pathExpression = series.name
        values = [transform(v) for v in series]
        series.extend(values)
        del series[:len(values)]
    returnValue(seriesList)


@inlineCallbacks
def isNonNull(requestContext, seriesList):
    """
    Takes a metric or wild card seriesList and counts up how many
    non-null values are specified. This is useful for understanding
    which metrics have data at a given point in time (ie, to count
    which servers are alive).

    Example:

    .. code-block:: none

      &target=isNonNull(webapp.pages.*.views)

    Returns a seriesList where 1 is specified for non-null values, and
    0 is specified for null values.
    """

    yield defer.succeed(None)

    def transform(v):
        if v is None:
            return 0
        else:
            return 1

    for series in seriesList:
        series.name = "isNonNull(%s)" % (series.name)
        series.pathExpression = series.name
        values = [transform(v) for v in series]
        series.extend(values)
        del series[:len(values)]
    returnValue(seriesList)


@inlineCallbacks
def identity(requestContext, name):
    """
    Identity function:
    Returns datapoints where the value equals the timestamp of the datapoint.
    Useful when you have another series where the value is a timestamp, and
    you want to compare it to the time of the datapoint, to render an age

    Example:

    .. code-block:: none

      &target=identity("The.time.series")

    This would create a series named "The.time.series" that contains points where
    x(t) == t.
    """
    yield defer.succeed(None)
    step = 60
    start = int(epoch(requestContext["startTime"]))
    end = int(epoch(requestContext["endTime"]))
    values = range(start, end, step)
    series = TimeSeries(name, start, end, step, values)
    series.pathExpression = 'identity("%s")' % name

    returnValue([series])


@inlineCallbacks
def countSeries(requestContext, *seriesLists):
    """
    Draws a horizontal line representing the number of nodes found in the seriesList.

    .. code-block:: none

      &target=countSeries(carbon.agents.*.*)

    """
    yield defer.succeed(None)
    (seriesList, start, end, step) = normalize(seriesLists)
    name = "countSeries(%s)" % formatPathExpressions(seriesList)
    values = (int(len(row)) for row in izip(*seriesList))
    series = TimeSeries(name, start, end, step, values)
    series.pathExpression = name
    returnValue([series])


def group(requestContext, *seriesLists):
    """
    Takes an arbitrary number of seriesLists and adds them to a single seriesList. This is used
    to pass multiple seriesLists to a function which only takes one
    """
    seriesGroup = []
    for s in seriesLists:
        seriesGroup.extend(s)

    return seriesGroup


@inlineCallbacks
def mapSeries(requestContext, seriesList, mapNode):
    """
    Short form: ``map()``

    Takes a seriesList and maps it to a list of seriesList. Each seriesList has the
    given mapNode in common.

    .. note:: This function is not very useful alone. It should be used with :py:func:`reduceSeries`

    .. code-block:: none

      mapSeries(servers.*.cpu.*,1) =>

        [
          servers.server1.cpu.*,
          servers.server2.cpu.*,
          ...
          servers.serverN.cpu.*
        ]
    """
    yield defer.succeed(None)
    metaSeries = {}
    keys = []
    for series in seriesList:
        key = series.name.split(".")[mapNode]
        if key not in metaSeries:
            metaSeries[key] = [series]
            keys.append(key)
        else:
            metaSeries[key].append(series)
    returnValue([metaSeries[k] for k in keys])


@inlineCallbacks
def reduceSeries(
        requestContext,
        seriesLists,
        reduceFunction,
        reduceNode,
        *reduceMatchers):
    """
    Short form: ``reduce()``

    Takes a list of seriesLists and reduces it to a list of series by means of the reduceFunction.

    Reduction is performed by matching the reduceNode in each series against the list of
    reduceMatchers. Then each series is passed to the reduceFunction as arguments in the order
    given by reduceMatchers. The reduceFunction should yield a single series.

    The resulting list of series are aliased so that they can easily be nested in other functions.

    **Example**: Map/Reduce asPercent(bytes_used,total_bytes) for each server

    Assume that metrics in the form below exist:

    .. code-block:: none

         servers.server1.disk.bytes_used
         servers.server1.disk.total_bytes
         servers.server2.disk.bytes_used
         servers.server2.disk.total_bytes
         servers.server3.disk.bytes_used
         servers.server3.disk.total_bytes
         ...
         servers.serverN.disk.bytes_used
         servers.serverN.disk.total_bytes

    To get the percentage of disk used for each server:

    .. code-block:: none

        reduceSeries(mapSeries(servers.*.disk.*,1),"asPercent",3,"bytes_used","total_bytes") =>

          alias(asPercent(servers.server1.disk.bytes_used,servers.server1.disk.total_bytes),"servers.server1.disk.reduce.asPercent"),
          alias(asPercent(servers.server2.disk.bytes_used,servers.server2.disk.total_bytes),"servers.server2.disk.reduce.asPercent"),
          alias(asPercent(servers.server3.disk.bytes_used,servers.server3.disk.total_bytes),"servers.server3.disk.reduce.asPercent"),
          ...
          alias(asPercent(servers.serverN.disk.bytes_used,servers.serverN.disk.total_bytes),"servers.serverN.disk.reduce.asPercent")

    In other words, we will get back the following metrics::

        servers.server1.disk.reduce.asPercent
        servers.server2.disk.reduce.asPercent
        servers.server3.disk.reduce.asPercent
        ...
        servers.serverN.disk.reduce.asPercent

    .. seealso:: :py:func:`mapSeries`

    """
    yield defer.succeed(None)
    metaSeries = {}
    keys = []
    for seriesList in seriesLists:
        for series in seriesList:
            nodes = series.name.split('.')
            node = nodes[reduceNode]
            reduceSeriesName = '.'.join(
                nodes[
                    0:reduceNode]) + '.reduce.' + reduceFunction
            if node in reduceMatchers:
                if reduceSeriesName not in metaSeries:
                    metaSeries[reduceSeriesName] = [None] * len(reduceMatchers)
                    keys.append(reduceSeriesName)
                i = reduceMatchers.index(node)
                metaSeries[reduceSeriesName][i] = series
    for key in keys:
        metaSeries[key] = (yield SeriesFunctions[reduceFunction](requestContext, *[[l] for l in metaSeries[key]]))[0]
        metaSeries[key].name = key
    returnValue([metaSeries[key] for key in keys])


@inlineCallbacks
def groupByNode(requestContext, seriesList, nodeNum, callback):
    """
    Takes a serieslist and maps a callback to subgroups within as defined by a common node

    .. code-block:: none

      &target=groupByNode(ganglia.by-function.*.*.cpu.load5,2,"sumSeries")

    Would return multiple series which are each the result of applying the "sumSeries" function
    to groups joined on the second node (0 indexed) resulting in a list of targets like

    .. code-block :: none

      sumSeries(ganglia.by-function.server1.*.cpu.load5),sumSeries(ganglia.by-function.server2.*.cpu.load5),...

    """
    yield defer.succeed(None)
    metaSeries = {}
    keys = []
    for series in seriesList:
        key = series.name.split(".")[nodeNum]
        if key not in metaSeries:
            metaSeries[key] = [series]
            keys.append(key)
        else:
            metaSeries[key].append(series)
    for key in metaSeries.keys():
        metaSeries[key] = (yield SeriesFunctions[callback](requestContext,
                                                           metaSeries[key]))[0]
        metaSeries[key].name = key
    returnValue([metaSeries[key] for key in keys])


@inlineCallbacks
def exclude(requestContext, seriesList, pattern):
    """
    Takes a metric or a wildcard seriesList, followed by a regular expression
    in double quotes.  Excludes metrics that match the regular expression.

    Example:

    .. code-block:: none

      &target=exclude(servers*.instance*.threads.busy,"server02")
    """
    yield defer.succeed(None)
    regex = re.compile(pattern)
    returnValue([s for s in seriesList if not regex.search(s.name)])


@inlineCallbacks
def grep(requestContext, seriesList, pattern):
    """
    Takes a metric or a wildcard seriesList, followed by a regular expression
    in double quotes.  Excludes metrics that don't match the regular expression.

    Example:

    .. code-block:: none

      &target=grep(servers*.instance*.threads.busy,"server02")
    """
    yield defer.succeed(None)
    regex = re.compile(pattern)
    returnValue([s for s in seriesList if regex.search(s.name)])


@inlineCallbacks
def smartSummarize(
        requestContext,
        seriesList,
        intervalString,
        func='sum',
        alignToFrom=False):
    """
    Smarter experimental version of summarize.

    The alignToFrom parameter has been deprecated, it no longer has any effect.
    Alignment happens automatically for days, hours, and minutes.
    """

    yield defer.succeed(None)
    results = []
    delta = parseTimeOffset(intervalString)
    interval = delta.seconds + (delta.days * 86400)

    # Adjust the start time to fit an entire day for intervals >= 1 day
    requestContext['delta'] = interval
    requestContext = requestContext.copy()
    s = requestContext['startTime']
    if interval >= DAY:
        requestContext['startTime'] = datetime(s.year, s.month, s.day)
    elif interval >= HOUR:
        requestContext['startTime'] = datetime(s.year, s.month, s.day, s.hour)
    elif interval >= MINUTE:
        requestContext['startTime'] = datetime(
            s.year,
            s.month,
            s.day,
            s.hour,
            s.minute)

    for i, series in enumerate(seriesList):
        # XXX: breaks with summarize(metric.{a,b})
        #      each series.pathExpression == metric.{a,b}
        newSeries = yield evaluateTarget(requestContext, series.pathExpression)[0]
        series[0:len(series)] = newSeries
        series.start = newSeries.start
        series.end = newSeries.end
        series.step = newSeries.step

    for series in seriesList:
        buckets = {}  # { timestamp: [values] }

        timestamps = range(int(series.start),
                           int(series.end),
                           int(series.step))
        datapoints = zip(timestamps, series)

        # Populate buckets
        for timestamp_, value in datapoints:
            bucketInterval = int((timestamp_ - series.start) / interval)

            if bucketInterval not in buckets:
                buckets[bucketInterval] = []

            if value is not None:
                buckets[bucketInterval].append(value)

        newValues = []
        for timestamp_ in range(series.start, series.end, interval):
            bucketInterval = int((timestamp_ - series.start) / interval)
            bucket = buckets.get(bucketInterval, [])

            if bucket:
                if func == 'avg':
                    newValues.append(float(sum(bucket)) / float(len(bucket)))
                elif func == 'last':
                    newValues.append(bucket[len(bucket) - 1])
                elif func == 'max':
                    newValues.append(max(bucket))
                elif func == 'min':
                    newValues.append(min(bucket))
                else:
                    newValues.append(sum(bucket))
            else:
                newValues.append(None)

        newName = "smartSummarize(%s, \"%s\", \"%s\")" % (
            series.name, intervalString, func)
        alignedEnd = series.start + (bucketInterval * interval) + interval
        newSeries = TimeSeries(
            newName,
            series.start,
            alignedEnd,
            interval,
            newValues)
        newSeries.pathExpression = newName
        results.append(newSeries)

    returnValue(results)


@inlineCallbacks
def summarize(
        requestContext,
        seriesList,
        intervalString,
        func='sum',
        alignToFrom=False):
    """
    Summarize the data into interval buckets of a certain size.

    By default, the contents of each interval bucket are summed together. This is
    useful for counters where each increment represents a discrete event and
    retrieving a "per X" value requires summing all the events in that interval.

    Specifying 'avg' instead will return the mean for each bucket, which can be more
    useful when the value is a gauge that represents a certain value in time.

    'max', 'min' or 'last' can also be specified.

    By default, buckets are caculated by rounding to the nearest interval. This
    works well for intervals smaller than a day. For example, 22:32 will end up
    in the bucket 22:00-23:00 when the interval=1hour.

    Passing alignToFrom=true will instead create buckets starting at the from
    time. In this case, the bucket for 22:32 depends on the from time. If
    from=6:30 then the 1hour bucket for 22:32 is 22:30-23:30.

    Example:

    .. code-block:: none

      &target=summarize(counter.errors, "1hour") # total errors per hour
      &target=summarize(nonNegativeDerivative(gauge.num_users), "1week") # new users per week
      &target=summarize(queue.size, "1hour", "avg") # average queue size per hour
      &target=summarize(queue.size, "1hour", "max") # maximum queue size during each hour
      &target=summarize(metric, "13week", "avg", true)&from=midnight+20100101 # 2010 Q1-4
    """
    yield defer.succeed(None)
    results = []
    delta = parseTimeOffset(intervalString)
    interval = delta.seconds + (delta.days * 86400)
    requestContext['delta'] = interval

    for series in seriesList:
        buckets = {}

        timestamps = range(int(series.start),
                           int(series.end) + int(series.step),
                           int(series.step))
        datapoints = zip(timestamps, series)

        for timestamp_, value in datapoints:
            if alignToFrom:
                bucketInterval = int((timestamp_ - series.start) / interval)
            else:
                bucketInterval = timestamp_ - (timestamp_ % interval)

            if bucketInterval not in buckets:
                buckets[bucketInterval] = []

            if value is not None:
                buckets[bucketInterval].append(value)

        if alignToFrom:
            newStart = series.start
            newEnd = series.end
        else:
            newStart = series.start - (series.start % interval)
            newEnd = series.end - (series.end % interval) + interval

        newValues = []
        for timestamp_ in range(newStart, newEnd, interval):
            if alignToFrom:
                newEnd = timestamp_
                bucketInterval = int((timestamp_ - series.start) / interval)
            else:
                bucketInterval = timestamp_ - (timestamp_ % interval)

            bucket = buckets.get(bucketInterval, [])

            if bucket:
                if func == 'avg':
                    newValues.append(float(sum(bucket)) / float(len(bucket)))
                elif func == 'last':
                    newValues.append(bucket[len(bucket) - 1])
                elif func == 'max':
                    newValues.append(max(bucket))
                elif func == 'min':
                    newValues.append(min(bucket))
                else:
                    newValues.append(sum(bucket))
            else:
                newValues.append(None)

        if alignToFrom:
            newEnd += interval

        newName = "summarize(%s, \"%s\", \"%s\"%s)" % (
            series.name, intervalString, func, alignToFrom and ", true" or "")
        newSeries = TimeSeries(newName, newStart, newEnd, interval, newValues)
        newSeries.pathExpression = newName
        results.append(newSeries)

    returnValue(results)


@inlineCallbacks
def hitcount(
        requestContext,
        seriesList,
        intervalString,
        alignToInterval=False):
    """
    Estimate hit counts from a list of time series.

    This function assumes the values in each time series represent
    hits per second.  It calculates hits per some larger interval
    such as per day or per hour.  This function is like summarize(),
    except that it compensates automatically for different time scales
    (so that a similar graph results from using either fine-grained
    or coarse-grained records) and handles rarely-occurring events
    gracefully.
    """
    yield defer.succeed(None)
    results = []
    delta = parseTimeOffset(intervalString)
    interval = int(delta.seconds + (delta.days * 86400))
    requestContext['delta'] = interval

    if alignToInterval:
        requestContext = requestContext.copy()
        s = requestContext['startTime']
        if interval >= DAY:
            requestContext['startTime'] = datetime(s.year, s.month, s.day)
        elif interval >= HOUR:
            requestContext['startTime'] = datetime(
                s.year,
                s.month,
                s.day,
                s.hour)
        elif interval >= MINUTE:
            requestContext['startTime'] = datetime(
                s.year,
                s.month,
                s.day,
                s.hour,
                s.minute)

        for i, series in enumerate(seriesList):
            newSeries = yield evaluateTarget(requestContext, series.pathExpression)[0]
            intervalCount = int((series.end - series.start) / interval)
            series[0:len(series)] = newSeries
            series.start = newSeries.start
            series.end = newSeries.start + \
                (intervalCount * interval) + interval
            series.step = newSeries.step

    for series in seriesList:
        step = int(series.step)
        bucket_count = int(
            math.ceil(
                float(
                    series.end -
                    series.start) /
                interval))
        buckets = [[] for _ in range(bucket_count)]
        newStart = int(series.end - bucket_count * interval)

        for i, value in enumerate(series):
            if value is None:
                continue

            start_time = int(series.start + i * step)
            start_bucket, start_mod = divmod(start_time - newStart, interval)
            end_time = start_time + step
            end_bucket, end_mod = divmod(end_time - newStart, interval)

            if end_bucket >= bucket_count:
                end_bucket = bucket_count - 1
                end_mod = interval

            if start_bucket == end_bucket:
                # All of the hits go to a single bucket.
                if start_bucket >= 0:
                    buckets[start_bucket].append(value * (end_mod - start_mod))

            else:
                # Spread the hits among 2 or more buckets.
                if start_bucket >= 0:
                    buckets[start_bucket].append(
                        value * (interval - start_mod))
                hits_per_bucket = value * interval
                for j in range(start_bucket + 1, end_bucket):
                    buckets[j].append(hits_per_bucket)
                if end_mod > 0:
                    buckets[end_bucket].append(value * end_mod)

        newValues = []
        for bucket in buckets:
            if bucket:
                newValues.append(sum(bucket))
            else:
                newValues.append(None)

        newName = 'hitcount(%s, "%s"%s)' % (
            series.name, intervalString, alignToInterval and ", true" or "")
        newSeries = TimeSeries(
            newName,
            newStart,
            series.end,
            interval,
            newValues)
        newSeries.pathExpression = newName
        results.append(newSeries)

    returnValue(results)


@inlineCallbacks
def timeFunction(requestContext, name, step=60):
    """
    Short Alias: time()

    Just returns the timestamp for each X value. T

    Example:

    .. code-block:: none

      &target=time("The.time.series")

    This would create a series named "The.time.series" that contains in Y the same
    value (in seconds) as X.
    Accepts optional second argument as 'step' parameter (default step is 60 sec)

    """
    yield defer.succeed(None)
    delta = timedelta(seconds=step)
    when = requestContext["startTime"]
    values = []

    while when < requestContext["endTime"]:
        values.append(time.mktime(when.timetuple()))
        when += delta

    series = TimeSeries(
        name, int(
            time.mktime(
                requestContext["startTime"].timetuple())), int(
            time.mktime(
                requestContext["endTime"].timetuple())), step, values)
    series.pathExpression = name

    returnValue([series])


@inlineCallbacks
def sinFunction(requestContext, name, amplitude=1, step=60):
    """
    Short Alias: sin()

    Just returns the sine of the current time. The optional amplitude parameter
    changes the amplitude of the wave.

    Example:

    .. code-block:: none

      &target=sin("The.time.series", 2)

    This would create a series named "The.time.series" that contains sin(x)*2.
    Accepts optional second argument as 'amplitude' parameter (default amplitude is 1)
    Accepts optional third argument as 'step' parameter (default step is 60 sec)
    """
    yield defer.succeed(None)
    delta = timedelta(seconds=step)
    when = requestContext["startTime"]
    values = []

    while when < requestContext["endTime"]:
        values.append(math.sin(time.mktime(when.timetuple())) * amplitude)
        when += delta

    returnValue([TimeSeries(name,
                            int(epoch(requestContext["startTime"])),
                            int(epoch(requestContext["endTime"])),
                            step, values)])


@inlineCallbacks
def removeEmptySeries(requestContext, seriesList):
    """
    Takes one metric or a wildcard seriesList.
    Out of all metrics passed, draws only the metrics with not empty data

    Example:

    .. code-block:: none

      &target=removeEmptySeries(server*.instance*.threads.busy)

    Draws only live servers with not empty data.

    """
    yield defer.succeed(None)
    returnValue([series for series in seriesList if safeIsNotEmpty(series)])


@inlineCallbacks
def randomWalkFunction(requestContext, name, step=60):
    """
    Short Alias: randomWalk()

    Returns a random walk starting at 0. This is great for testing when there is
    no real data in whisper.

    Example:

    .. code-block:: none

      &target=randomWalk("The.time.series")

    This would create a series named "The.time.series" that contains points where
    x(t) == x(t-1)+random()-0.5, and x(0) == 0.
    Accepts optional second argument as 'step' parameter (default step is 60 sec)
    """
    yield defer.succeed(None)
    delta = timedelta(seconds=step)
    when = requestContext["startTime"]
    values = []
    current = 0
    while when < requestContext["endTime"]:
        values.append(current)
        current += random.random() - 0.5
        when += delta

    returnValue([TimeSeries(name,
                            int(epoch(requestContext["startTime"])),
                            int(epoch(requestContext["endTime"])),
                            step, values)])


def pieAverage(requestContext, series):
    return safeDiv(safeSum(series), safeLen(series))


def pieMaximum(requestContext, series):
    return max(series)


def pieMinimum(requestContext, series):
    return min(series)

PieFunctions = {
    'average': pieAverage,
    'maximum': pieMaximum,
    'minimum': pieMinimum,
}

SeriesFunctions = {
    # Combine functions
    'sumSeries': sumSeries,
    'sum': sumSeries,
    'multiplySeries': multiplySeries,
    'averageSeries': averageSeries,
    'stddevSeries': stddevSeries,
    'avg': averageSeries,
    'sumSeriesWithWildcards': sumSeriesWithWildcards,
    'averageSeriesWithWildcards': averageSeriesWithWildcards,
    'multiplySeriesWithWildcards': multiplySeriesWithWildcards,
    'minSeries': minSeries,
    'maxSeries': maxSeries,
    'rangeOfSeries': rangeOfSeries,
    'percentileOfSeries': percentileOfSeries,
    'countSeries': countSeries,
    'weightedAverage': weightedAverage,

    # Transform functions
    'scale': scale,
    'scaleToSeconds': scaleToSeconds,
    'offset': offset,
    'offsetToZero': offsetToZero,
    'derivative': derivative,
    'squareRoot': squareRoot,
    'pow': pow,
    'perSecond': perSecond,
    'integral': integral,
    'percentileOfSeries': percentileOfSeries,
    'nonNegativeDerivative': nonNegativeDerivative,
    'log': logarithm,
    'invert': invert,
    'timeStack': timeStack,
    'timeShift': timeShift,
    'timeSlice': timeSlice,
    'summarize': summarize,
    'smartSummarize': smartSummarize,
    'hitcount': hitcount,
    'absolute': absolute,

    # Calculate functions
    'movingAverage': movingAverage,
    'movingMedian': movingMedian,
    'stdev': stdev,
    'holtWintersForecast': holtWintersForecast,
    'holtWintersConfidenceBands': holtWintersConfidenceBands,
    'holtWintersConfidenceArea': holtWintersConfidenceArea,
    'holtWintersAberration': holtWintersAberration,
    'asPercent': asPercent,
    'pct': asPercent,
    'diffSeries': diffSeries,
    'divideSeries': divideSeries,

    # Series Filter functions
    'fallbackSeries': fallbackSeries,
    'mostDeviant': mostDeviant,
    'highestCurrent': highestCurrent,
    'lowestCurrent': lowestCurrent,
    'highestMax': highestMax,
    'currentAbove': currentAbove,
    'currentBelow': currentBelow,
    'highestAverage': highestAverage,
    'lowestAverage': lowestAverage,
    'averageAbove': averageAbove,
    'averageBelow': averageBelow,
    'maximumAbove': maximumAbove,
    'minimumAbove': minimumAbove,
    'maximumBelow': maximumBelow,
    'minimumBelow': minimumBelow,
    'nPercentile': nPercentile,
    'limit': limit,
    'sortByTotal': sortByTotal,
    'sortByName': sortByName,
    'averageOutsidePercentile': averageOutsidePercentile,
    'removeBetweenPercentile': removeBetweenPercentile,
    'sortByMaxima': sortByMaxima,
    'sortByMinima': sortByMinima,
    'useSeriesAbove': useSeriesAbove,
    'exclude': exclude,
    'grep': grep,
    'removeEmptySeries': removeEmptySeries,

    # Data Filter functions
    'removeAbovePercentile': removeAbovePercentile,
    'removeAboveValue': removeAboveValue,
    'removeBelowPercentile': removeBelowPercentile,
    'removeBelowValue': removeBelowValue,

    # Special functions
    'legendValue': legendValue,
    'alias': alias,
    'aliasSub': aliasSub,
    'aliasByNode': aliasByNode,
    'aliasByMetric': aliasByMetric,
    'cactiStyle': cactiStyle,
    'color': color,
    'alpha': alpha,
    'cumulative': cumulative,
    'consolidateBy': consolidateBy,
    'keepLastValue': keepLastValue,
    'changed': changed,
    'drawAsInfinite': drawAsInfinite,
    'secondYAxis': secondYAxis,
    'lineWidth': lineWidth,
    'dashed': dashed,
    'substr': substr,
    'group': group,
    'map': mapSeries,
    'mapSeries': mapSeries,
    'reduce': reduceSeries,
    'reduceSeries': reduceSeries,
    'groupByNode': groupByNode,
    'constantLine': constantLine,
    'stacked': stacked,
    'areaBetween': areaBetween,
    'threshold': threshold,
    'transformNull': transformNull,
    'isNonNull': isNonNull,
    'identity': identity,
    'aggregateLine': aggregateLine,

    # test functions
    'time': timeFunction,
    "sin": sinFunction,
    "randomWalk": randomWalkFunction,
    'timeFunction': timeFunction,
    "sinFunction": sinFunction,
    "randomWalkFunction": randomWalkFunction,
}


# Avoid import circularity
from moira.graphite.evaluator import evaluateTarget
