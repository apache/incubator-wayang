<!---
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
--->

# PyWayang

Implementation of a python API for Apache Wayang

## Building from source
To build and install `pywy` as a python library, `build` is needed. It
can be installed using:
```shell
cd ./python
pip install --upgrade build
```
Installing this might require `python3.8-venv` to be installed on the
system.

After building the python package, execute the following steps to make
it available for your system:
```shell
python3 -m pip install dist/pywy-0.7.1.tar.gz
```

## Executing python code
In order to execute python code, the REST API needs to be running.
Compiling the assembly and executing the Main class for the REST API can
be done with the following steps:


Before compiling your code, make sure the required configuration variables
are set correctly in `wayang-api/wayang-api-python/src/main/resources/wayang-api-python-defaults.properties`.
This example `wayang-api-python-defaults.properties` can be used as a
guideline:

```
wayang.api.python.worker = /var/www/html/python/src/pywy/execution/worker.py
wayang.api.python.path = python3
wayang.api.python.env.path = /usr/local/lib/python3.8/dist-packages
```
The first configuration value needs to point to the location of your
apache wayang repository so that it can find the python worker that
executes UDFs (usually your current work directory +
`python/src/pypy/execution/worker.py`)
The second value is your command used for invoking python scripts.
Usually either python3 or just python.
The third value points to the directory in which python libraries are to
be found (usually where pip installs them).

- Package the project
```shell
./mvnw clean package -pl :wayang-assembly -Pdistribution
```

- Starting the REST API as a background process
```shell
cd wayang-assembly/target/
tar -xvf apache-wayang-assembly-0.7.1-SNAPSHOT-incubating-dist.tar.gz
cd wayang-0.7.1-SNAPSHOT
./bin/wayang-submit org.apache.wayang.api.json.Main &
```

Now, create and execute a python script like this:

```python
from pywy.dataquanta import WayangContext
from pywy.platforms.java import JavaPlugin
from pywy.platforms.spark import SparkPlugin

def word_count():
    ctx = WayangContext() \
        .register({JavaPlugin, SparkPlugin}) \
        .textfile("file:///README.md") \
        .flatmap(lambda w: w.split()) \
        .filter(lambda w: w.strip() != "") \
        .map(lambda w: (w.lower(), 1)) \
        .reduce_by_key(lambda t: t[0], lambda t1, t2: (t1[0], int(t1[1]) + int(t2[1]))) \
        .store_textfile("file:///wordcount-out-python.txt")

if __name__ == "__main__":
    word_count
```
