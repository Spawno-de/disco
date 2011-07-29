from disco.test import TestCase, TestJob
from disco.util import kvgroup

class CounterJob(TestJob):
    @staticmethod
    def map(line, params):
        from disco.counters import Counter
        c = Counter("map yields")
        for word in line.lower().split():
            c.inrement()
            yield word, 1

    @staticmethod
    def reduce(iter, params):
        from disco.counters import Counter
        c = Counter("reduce yields")
        for k, vs in kvgroup:
            c.inrement()
            yield k, sum(int(v) for v in vs)

class CounterTestCase(TestCase):
    @property
    def partitions(self):
        return min(self.num_workers * 10, 300)

    def serve(self, path):
        return "Gutta cavat cavat lapidem\n" * 100

    def test_five(self):
        self.job = CounterJob().run(input=self.test_server.urls([''] * 5),
                                    partitions=self.partitions)
        self.job.wait(show=False)
        print self.job.counters()
#        self.assertEquals(dict(self.results(self.job)),
#                          {'gutta':   int(5e2),
#                           'cavat':   int(1e3),
#                           'lapidem': int(5e2)})
    
