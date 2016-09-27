from twisted.trial import unittest
from moira.cache import cache
from twisted.internet.defer import inlineCallbacks


class Cache(unittest.TestCase):

    @cache
    @inlineCallbacks
    def function(self, items):
        items.append(1)
        yield None
        
    @inlineCallbacks
    def testCache(self):
        items = []
        yield self.function(items)
        yield self.function(items, cache_key=1, cache_ttl=10)
        self.assertEqual(len(items), 2)
        yield self.function(items, cache_key=1, cache_ttl=10)
        self.assertEqual(len(items), 2)
