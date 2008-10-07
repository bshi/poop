# License: Public Domain
import string
import sys
import poop

# Sample command line testing invokation:
# $ cat data.txt | \
#         python wc.py MAP WordCount | sort | \
#         python wc.py REDUCE WordCount | \
#         python wc.py MAP UniqueCount | sort \
#         python wc.py REDUCE UniqueCount

class WordCount(poop.PoopJob):
    def setup(self):
        print >>sys.stderr, "WordCount().setup()"

    def map(self, key, val):
        val = filter(lambda x: x not in string.punctuation, val)
        for w in val.split(): yield (w.lower(), 1)

    def postmap(self):
        print >>sys.stderr, "WordCount().postmap()"

    def reduce(self, key, vals):
        yield (key, sum(map(int, vals)))

    def postreduce(self):
        print >>sys.stderr, "WordCount().postreduce()"


class UniqueCount(poop.PoopJob):
    @staticmethod
    def map(key, val):
        yield ('unique words', 1)

    @staticmethod
    def reduce(key, vals):
        yield (key, sum(map(int, vals)))


WordCount.child = UniqueCount
poop.run(sys.argv, WordCount)
