from mrjob.job import MRJob
import json

class PurchaseCount(MRJob):
    """
    Trabajo de Map-Reduce Job para conteo de compras.
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