from mrjob.job import MRJob
import json

class PurchaseCount(MRJob):
    """
    Map-Reduce Job for counting purchases.
    """

    def mapper(self, _, line):
        data = json.loads(line)
        item = data.get("item")
        if item:
            yield item, 1

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    PurchaseCount.run()