#
#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#


import os
import subprocess
import time
import pytest

from importlib import resources
from typing import Tuple
from typing import Iterable
from pywy.dataquanta import WayangContext
from pywy.platforms.java import JavaPlugin
from pywy.platforms.spark import SparkPlugin
from pywy.tests import resources as resources_folder

def fm_func(w: str) -> Iterable[str]:
    return w.split()

def filter_func(w: str) -> bool:
    return w.strip() != ""

def map_func(w: str) -> Tuple[str, int]:
    return (w.lower(), 1)

def key_func(t: Tuple[str, int]) -> str:
    return t[0]

def reduce_func(t1: Tuple[str, int], t2: Tuple[str, int]) -> Tuple[str, int]:
    return (t1[0], int(t1[1]) + int(t2[1]))

def sort_func(tup: Tuple[str, int]):
    return tup[0]



@pytest.fixture(scope="session")
def config(pytestconfig):
    return pytestconfig.getoption("config")


# TODO: implement tuple types support
@pytest.mark.skip(reason="missing implementation for tuple types in operators")
def test_wordcount(config):
    with resources.path(resources_folder, "sample_data.md") as resource_path, \
         resources.path(resources_folder, "wordcount_out_python.txt") as output_path, \
         resources.path(resources_folder, "wayang.properties") as configuration_file_path:
        
        configuration_file_path = config if config is not None else configuration_file_path

        proc = subprocess.Popen([
            f"mvn", f"-q", f"-f", f"wayang-api/wayang-api-json/pom.xml", f"exec:java",
            f"-Dexec.mainClass=org.apache.wayang.api.json.Main", 
            f"-Dwayang.configuration=file://{configuration_file_path}", 
            f"-Dexec.args=8080"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=os.environ.copy(), cwd=os.getcwd())
        print(proc.stdout.readline()), print(proc.stdout.readline()), time.sleep(1) # wait for zio to print to output

        try:  
            # anonymous functions with type hints
            _ctx = WayangContext() \
                .register({JavaPlugin, SparkPlugin}) \
                .textfile(f"file://{resource_path}") \
                .flatmap(lambda w: w.split(), str, str) \
                .filter(lambda w: w.strip() != "", str) \
                .map(lambda w: (w.lower(), 1), str, (str, int)) \
                .reduce_by_key(lambda t: t[0], lambda t1, t2: (t1[0], int(t1[1]) + int(t2[1])), (str, int)) \
                .store_textfile(f"file://{output_path}", (str, int))
            
            # named functions with  signatures
            _ctx = WayangContext() \
                .register({JavaPlugin, SparkPlugin}) \
                .textfile(f"file://{resource_path}") \
                .flatmap(fm_func) \
                .filter(filter_func) \
                .map(map_func) \
                .reduce_by_key(key_func, reduce_func) \
                .sort(sort_func) \
                .store_textfile(f"file://{output_path}", (str, int))
        finally:
            proc.kill()