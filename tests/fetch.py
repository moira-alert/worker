import sys
import os
sys.path.insert(0,
                os.path.abspath(
                    os.path.join(
                        os.path.abspath(
                            os.path.dirname(__file__)),
                        '.')))
from twisted.trial import unittest
from moira.graphite.datalib import extract


def unpackTimeSeries(dataList, retention, startTime, endTime, allowRealTimeAlerting):
    valuesList = []
    for data in dataList:
        points = {}
        for value, timestamp in data:
            bucket = (int)((timestamp - startTime) / retention)
            points[bucket] = extract(value)

        lastFullBucketEndTime = ((endTime - startTime) / retention) * retention

        values = []
        for timestamp in range(startTime, lastFullBucketEndTime, retention):
            point = points.get((int)((timestamp - startTime) / retention))
            values.append(point)

        lastPoint = points.get((int)((endTime - startTime) / retention))
        if allowRealTimeAlerting and lastPoint is not None:
            values.append(lastPoint)

        valuesList.append(values)
    return valuesList


class Fetch(unittest.TestCase):
    def generateRedisDataPoint(self, timestamp, value):
        return ("%f %f" % (timestamp, value), timestamp)

    def testConservativeShiftedSeries(self):
        retention = 10
        startTime = 0
        dataList = [[]]

        # time == 0
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 0, allowRealTimeAlerting=False), [[]])

        # time == 5
        dataList[0].append(self.generateRedisDataPoint(5, 100.))
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 5, allowRealTimeAlerting=False), [[]])

        # time == 9, 10, 11
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 9, allowRealTimeAlerting=False), [[]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 10, allowRealTimeAlerting=False), [[100.]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 11, allowRealTimeAlerting=False), [[100.]])

        # time == 15
        dataList[0].append(self.generateRedisDataPoint(15, 200.))

        # time == 25
        dataList[0].append(self.generateRedisDataPoint(25, 300.))
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 25, allowRealTimeAlerting=False), [[100., 200.]])

        # time == 29, 30
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 29, allowRealTimeAlerting=False), [[100., 200.]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 30, allowRealTimeAlerting=False), [[100., 200., 300.]])

    def testRealTimeShiftedSeries(self):
        retention = 10
        startTime = 0
        dataList = [[]]

        # time == 0
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 0, allowRealTimeAlerting=True), [[]])

        # time == 5
        dataList[0].append(self.generateRedisDataPoint(5, 100.))
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 5, allowRealTimeAlerting=True), [[100.]])

        # time == 9, 10, 11
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 9, allowRealTimeAlerting=True), [[100.]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 10, allowRealTimeAlerting=True), [[100.]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 11, allowRealTimeAlerting=True), [[100.]])

        # time == 15
        dataList[0].append(self.generateRedisDataPoint(15, 200.))

        # time == 25
        dataList[0].append(self.generateRedisDataPoint(25, 300.))
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 25, allowRealTimeAlerting=True), [[100., 200., 300.]])

        # time == 29, 30
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 29, allowRealTimeAlerting=True), [[100., 200., 300.]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 30, allowRealTimeAlerting=True), [[100., 200., 300.]])

    def testConservativeAlignedSeries(self):
        retention = 10
        startTime = 0
        dataList = [[]]

        # time == 0
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 0, allowRealTimeAlerting=False), [[]])
        dataList[0].append(self.generateRedisDataPoint(0, 100.))
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 0, allowRealTimeAlerting=False), [[]])

        # time == 9, 10
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 9, allowRealTimeAlerting=False), [[]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 10, allowRealTimeAlerting=False), [[100.]])
        dataList[0].append(self.generateRedisDataPoint(10, 200.))

        # time == 20
        dataList[0].append(self.generateRedisDataPoint(20, 300.))
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 20, allowRealTimeAlerting=False), [[100., 200.]])

        # time == 29, 30
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 29, allowRealTimeAlerting=False), [[100., 200.]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 30, allowRealTimeAlerting=False), [[100., 200., 300.]])

    def testRealtimeAlignedSeries(self):
        retention = 10
        startTime = 0
        dataList = [[]]

        # time == 0
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 0, allowRealTimeAlerting=True), [[]])
        dataList[0].append(self.generateRedisDataPoint(0, 100.))
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 0, allowRealTimeAlerting=True), [[100.]])

        # time == 9, 10
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 9, allowRealTimeAlerting=True), [[100.]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 10, allowRealTimeAlerting=True), [[100.]])
        dataList[0].append(self.generateRedisDataPoint(10, 200.))

        # time == 20
        dataList[0].append(self.generateRedisDataPoint(20, 300.))
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 20, allowRealTimeAlerting=True), [[100., 200., 300.]])

        # time == 29, 30
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 29, allowRealTimeAlerting=True), [[100., 200., 300.]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 30, allowRealTimeAlerting=True), [[100., 200., 300.]])

    def testNodataSeries(self):
        retention = 10
        startTime = 0
        dataList = [[]]

        # time == 0
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 0, allowRealTimeAlerting=False), [[]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 0, allowRealTimeAlerting=True), [[]])

        # time == 9, 10, 11
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 9, allowRealTimeAlerting=False), [[]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 9, allowRealTimeAlerting=True), [[]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 10, allowRealTimeAlerting=False), [[None]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 10, allowRealTimeAlerting=True), [[None]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 11, allowRealTimeAlerting=False), [[None]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 11, allowRealTimeAlerting=True), [[None]])

        # time == 20
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 20, allowRealTimeAlerting=False), [[None, None]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 20, allowRealTimeAlerting=True), [[None, None]])

    def testConservativeMultipleSeries(self):
        retention = 10
        startTime = 0
        dataList = [[], []]

        # time == 0
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 0, allowRealTimeAlerting=False), [[], []])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 0, allowRealTimeAlerting=True), [[], []])
        dataList[0].append(self.generateRedisDataPoint(0, 100.))
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 0, allowRealTimeAlerting=False), [[], []])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 0, allowRealTimeAlerting=True), [[100.], []])

        # time == 5
        dataList[1].append(self.generateRedisDataPoint(5, 150.))
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 5, allowRealTimeAlerting=False), [[], []])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 5, allowRealTimeAlerting=True), [[100.], [150.]])

        # time == 9, 10
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 9, allowRealTimeAlerting=False), [[], []])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 9, allowRealTimeAlerting=True), [[100.], [150.]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 10, allowRealTimeAlerting=False), [[100.], [150.]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 10, allowRealTimeAlerting=True), [[100.], [150.]])
        dataList[0].append(self.generateRedisDataPoint(10, 200.))

        # time == 15
        dataList[1].append(self.generateRedisDataPoint(15, 250.))

        # time == 20
        dataList[0].append(self.generateRedisDataPoint(20, 300.))
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 20, allowRealTimeAlerting=False), [[100., 200.], [150.,250.]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 20, allowRealTimeAlerting=True), [[100., 200., 300.], [150.,250.]])

        # time == 25
        dataList[1].append(self.generateRedisDataPoint(25, 350.))

        # time == 29, 30
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 29, allowRealTimeAlerting=False), [[100., 200.], [150., 250.]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 29, allowRealTimeAlerting=True), [[100., 200., 300.], [150., 250., 350.]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 30, allowRealTimeAlerting=False), [[100., 200., 300.], [150., 250., 350.]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 30, allowRealTimeAlerting=True), [[100., 200., 300.], [150., 250., 350.]])

    def testNonZeroStartTimeSeries(self):
        retention = 10
        startTime = 1
        dataList = [[]]

        # time == 1
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 1, allowRealTimeAlerting=True), [[]])

        # time == 6
        dataList[0].append(self.generateRedisDataPoint(6, 100.))
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 6, allowRealTimeAlerting=True), [[100.]])

        # time == 10, 11, 12
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 10, allowRealTimeAlerting=True), [[100.]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 11, allowRealTimeAlerting=True), [[100.]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 12, allowRealTimeAlerting=True), [[100.]])

        # time == 16
        dataList[0].append(self.generateRedisDataPoint(16, 200.))

        # time == 26
        dataList[0].append(self.generateRedisDataPoint(26, 300.))
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 26, allowRealTimeAlerting=True), [[100., 200., 300.]])

        # time == 30, 31
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 30, allowRealTimeAlerting=True), [[100., 200., 300.]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 31, allowRealTimeAlerting=True), [[100., 200., 300.]])
