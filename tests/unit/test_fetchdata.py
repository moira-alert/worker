from twisted.trial import unittest
from moira.graphite.datalib import unpackTimeSeries


class FetchData(unittest.TestCase):
    def generateRedisDataPoint(self, timestamp, value):
        return ("%f %f" % (timestamp, value), timestamp)

    def testConservativeShiftedSeries(self):
        retention = 10
        startTime = 0
        dataList = [[]]

        # time == 0
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 0, bootstrap=False, allowRealTimeAlerting=False), [[]])

        # time == 5
        dataList[0].append(self.generateRedisDataPoint(5, 100.))
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 5, bootstrap=False, allowRealTimeAlerting=False), [[]])

        # time == 9, 10, 11
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 9, bootstrap=False, allowRealTimeAlerting=False), [[]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 10, bootstrap=False, allowRealTimeAlerting=False), [[100.]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 11, bootstrap=False, allowRealTimeAlerting=False), [[100.]])

        # time == 15
        dataList[0].append(self.generateRedisDataPoint(15, 200.))

        # time == 25
        dataList[0].append(self.generateRedisDataPoint(25, 300.))
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 25, bootstrap=False, allowRealTimeAlerting=False), [[100., 200.]])

        # time == 29, 30
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 29, bootstrap=False, allowRealTimeAlerting=False), [[100., 200.]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 30, bootstrap=False, allowRealTimeAlerting=False), [[100., 200., 300.]])

    def testRealTimeShiftedSeries(self):
        retention = 10
        startTime = 0
        dataList = [[]]

        # time == 0
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 0, bootstrap=False, allowRealTimeAlerting=True), [[]])

        # time == 5
        dataList[0].append(self.generateRedisDataPoint(5, 100.))
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 5, bootstrap=False, allowRealTimeAlerting=True), [[100.]])

        # time == 9, 10, 11
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 9, bootstrap=False, allowRealTimeAlerting=True), [[100.]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 10, bootstrap=False, allowRealTimeAlerting=True), [[100.]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 11, bootstrap=False, allowRealTimeAlerting=True), [[100.]])

        # time == 15
        dataList[0].append(self.generateRedisDataPoint(15, 200.))

        # time == 25
        dataList[0].append(self.generateRedisDataPoint(25, 300.))
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 25, bootstrap=False, allowRealTimeAlerting=True), [[100., 200., 300.]])

        # time == 29, 30
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 29, bootstrap=False, allowRealTimeAlerting=True), [[100., 200., 300.]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 30, bootstrap=False, allowRealTimeAlerting=True), [[100., 200., 300.]])

    def testConservativeAlignedSeries(self):
        retention = 10
        startTime = 0
        dataList = [[]]

        # time == 0
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 0, bootstrap=False, allowRealTimeAlerting=False), [[]])
        dataList[0].append(self.generateRedisDataPoint(0, 100.))
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 0, bootstrap=False, allowRealTimeAlerting=False), [[]])

        # time == 9, 10
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 9, bootstrap=False, allowRealTimeAlerting=False), [[]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 10, bootstrap=False, allowRealTimeAlerting=False), [[100.]])
        dataList[0].append(self.generateRedisDataPoint(10, 200.))

        # time == 20
        dataList[0].append(self.generateRedisDataPoint(20, 300.))
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 20, bootstrap=False, allowRealTimeAlerting=False), [[100., 200.]])

        # time == 29, 30
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 29, bootstrap=False, allowRealTimeAlerting=False), [[100., 200.]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 30, bootstrap=False, allowRealTimeAlerting=False), [[100., 200., 300.]])

    def testRealtimeAlignedSeries(self):
        retention = 10
        startTime = 0
        dataList = [[]]

        # time == 0
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 0, bootstrap=False, allowRealTimeAlerting=True), [[]])
        dataList[0].append(self.generateRedisDataPoint(0, 100.))
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 0, bootstrap=False, allowRealTimeAlerting=True), [[100.]])

        # time == 9, 10
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 9, bootstrap=False, allowRealTimeAlerting=True), [[100.]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 10, bootstrap=False, allowRealTimeAlerting=True), [[100.]])
        dataList[0].append(self.generateRedisDataPoint(10, 200.))

        # time == 20
        dataList[0].append(self.generateRedisDataPoint(20, 300.))
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 20, bootstrap=False, allowRealTimeAlerting=True), [[100., 200., 300.]])

        # time == 29, 30
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 29, bootstrap=False, allowRealTimeAlerting=True), [[100., 200., 300.]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 30, bootstrap=False, allowRealTimeAlerting=True), [[100., 200., 300.]])

    def testNodataSeries(self):
        retention = 10
        startTime = 0
        dataList = [[]]

        # time == 0
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 0, bootstrap=False, allowRealTimeAlerting=False), [[]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 0, bootstrap=False, allowRealTimeAlerting=True), [[]])

        # time == 9, 10, 11
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 9, bootstrap=False, allowRealTimeAlerting=False), [[]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 9, bootstrap=False, allowRealTimeAlerting=True), [[]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 10, bootstrap=False, allowRealTimeAlerting=False), [[None]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 10, bootstrap=False, allowRealTimeAlerting=True), [[None]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 11, bootstrap=False, allowRealTimeAlerting=False), [[None]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 11, bootstrap=False, allowRealTimeAlerting=True), [[None]])

        # time == 20
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 20, bootstrap=False, allowRealTimeAlerting=False), [[None, None]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 20, bootstrap=False, allowRealTimeAlerting=True), [[None, None]])

    def testConservativeMultipleSeries(self):
        retention = 10
        startTime = 0
        dataList = [[], []]

        # time == 0
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 0, bootstrap=False, allowRealTimeAlerting=False), [[], []])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 0, bootstrap=False, allowRealTimeAlerting=True), [[], []])
        dataList[0].append(self.generateRedisDataPoint(0, 100.))
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 0, bootstrap=False, allowRealTimeAlerting=False), [[], []])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 0, bootstrap=False, allowRealTimeAlerting=True), [[100.], []])

        # time == 5
        dataList[1].append(self.generateRedisDataPoint(5, 150.))
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 5, bootstrap=False, allowRealTimeAlerting=False), [[], []])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 5, bootstrap=False, allowRealTimeAlerting=True), [[100.], [150.]])

        # time == 9, 10
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 9, bootstrap=False, allowRealTimeAlerting=False), [[], []])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 9, bootstrap=False, allowRealTimeAlerting=True), [[100.], [150.]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 10, bootstrap=False, allowRealTimeAlerting=False), [[100.], [150.]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 10, bootstrap=False, allowRealTimeAlerting=True), [[100.], [150.]])
        dataList[0].append(self.generateRedisDataPoint(10, 200.))

        # time == 15
        dataList[1].append(self.generateRedisDataPoint(15, 250.))

        # time == 20
        dataList[0].append(self.generateRedisDataPoint(20, 300.))
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 20, bootstrap=False, allowRealTimeAlerting=False), [[100., 200.], [150.,250.]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 20, bootstrap=False, allowRealTimeAlerting=True), [[100., 200., 300.], [150.,250.]])

        # time == 25
        dataList[1].append(self.generateRedisDataPoint(25, 350.))

        # time == 29, 30
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 29, bootstrap=False, allowRealTimeAlerting=False), [[100., 200.], [150., 250.]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 29, bootstrap=False, allowRealTimeAlerting=True), [[100., 200., 300.], [150., 250., 350.]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 30, bootstrap=False, allowRealTimeAlerting=False), [[100., 200., 300.], [150., 250., 350.]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 30, bootstrap=False, allowRealTimeAlerting=True), [[100., 200., 300.], [150., 250., 350.]])

    def testNonZeroStartTimeSeries(self):
        retention = 10
        startTime = 2
        dataList = [[]]

        # time == 11
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 11, bootstrap=False, allowRealTimeAlerting=False), [[]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 11, bootstrap=False, allowRealTimeAlerting=True), [[]])
        dataList[0].append(self.generateRedisDataPoint(11, 100.))
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 11, bootstrap=False, allowRealTimeAlerting=False), [[]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 11, bootstrap=False, allowRealTimeAlerting=True), [[100.]])

        # time == 12
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 12, bootstrap=False, allowRealTimeAlerting=False), [[100.]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 12, bootstrap=False, allowRealTimeAlerting=True), [[100.]])


    def testBootstrapMode(self):
        retention = 10
        startTime = 0
        dataList = [[]]

        # time == 0, 10
        dataList[0].append(self.generateRedisDataPoint(0, 100.))
        dataList[0].append(self.generateRedisDataPoint(10, 200.))

        # time == 20
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 20, bootstrap=True, allowRealTimeAlerting=True), [[100., 200., None]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 20, bootstrap=True, allowRealTimeAlerting=False), [[100., 200., None]])
        dataList[0].append(self.generateRedisDataPoint(20, 300.))
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 20, bootstrap=True, allowRealTimeAlerting=True), [[100., 200., 300.]])
        self.assertEqual(unpackTimeSeries(dataList, retention, startTime, 20, bootstrap=True, allowRealTimeAlerting=False), [[100., 200., 300.]])
