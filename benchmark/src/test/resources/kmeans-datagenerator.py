#!/usr/bin/python

import random

class Distribution:
    """Represents a normal distribution"""

    def __init__(self, meanx, meany, stddev):
        self.meanx = meanx
        self.meany = meany
        self.stddev = stddev

    def sample(self):
        x = random.gauss(self.meanx, self.stddev)
        y = random.gauss(self.meany, self.stddev)
        return x, y


if __name__ == '__main__':
    import sys
    if len(sys.argv) == 1:
        print "Usage: python datagen.py <#points> <avg x,avg y,standard dev>+"
        sys.exit(1)

    numpoints = int(sys.argv[1])

    distributions = []
    for arg in sys.argv[2:]:
        parsedargs = map(float, arg.split(','))
        distributions.append(Distribution(*parsedargs))

    linecount = 0
    while linecount < numpoints:
        distribution = distributions[linecount % len(distributions)]
        x, y = distribution.sample()
        print '{:.5f},{:.5f}'.format(x, y)
        linecount += 1

