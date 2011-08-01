from disco.test import TestCase, TestJob
from disco.util import kvgroup
from disco.counter import Counter

class ManyMapJob(TestJob):
    sort = False

    @staticmethod
    def map(line, params):
        c = Counter("c")
        for word in line.lower().split():
            c.increment()
            yield word, 1

    @staticmethod
    def reduce(iter, params):
        c = Counter("c")
        for k, vs in kvgroup(sorted(iter)):
            c.increment()
            yield k, sum(int(v) for v in vs)

class ManyMapTestCase(TestCase):
    @property
    def partitions(self):
        return min(self.num_workers * 10, 300)

    def serve(self, path):
        return "Gutta cavat cavat lapidem\n" * 100

    def test_five(self):
        self.job = ManyMapJob().run(input=self.test_server.urls([''] * 5),
                                    partitions=self.partitions)
        self.assertEquals(dict(self.results(self.job)),
                          {'gutta':   int(5e2),
                           'cavat':   int(1e3),
                           'lapidem': int(5e2)})
        self.assertEquals(self.job.counters(), [{u'c': 2003}])
    
