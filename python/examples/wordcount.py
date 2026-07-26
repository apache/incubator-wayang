#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from pywy.dataquanta import WayangContext
from pywy.platforms.java import JavaPlugin


def word_count():
    WayangContext() \
        .register({JavaPlugin}) \
        .textfile("file:///opt/wayang/smoke/wordcount.txt") \
        .flatmap(lambda line: line.split(), str, str) \
        .filter(lambda word: word.strip() != "", str) \
        .map(lambda word: (word.lower(), 1), str, (str, int)) \
        .reduce_by_key(lambda item: item[0], lambda left, right: (left[0], int(left[1]) + int(right[1])), (str, int)) \
        .store_textfile("file:///tmp/wayang-python-wordcount.txt", (str, int))


if __name__ == "__main__":
    word_count()
