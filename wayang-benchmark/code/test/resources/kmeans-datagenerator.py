#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

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

