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

from pathlib import Path
import subprocess
import time
from pywy.dataquanta import WayangContext
from pywy.platforms.java import JavaPlugin
from pywy.platforms.spark import SparkPlugin
from pywy.tests import resources as resources_folder

from importlib import resources
from pathlib import Path

def test_filter_to_json():
    wayang_runner_dir = Path.cwd() / 'wayang-assembly' / 'target' / 'wayang-1.1.0' / 'bin'

    print("Opening subprocess")
    with resources.path(resources_folder, "sample_data.txt") as resource_path, \
         resources.path(resources_folder, "wordcount_out_python.txt") as output_path, \
         resources.path(resources_folder, "wayang.properties") as configuration_file_path:
        print(f"Using resource path: {resource_path}")
        print(f"Using output path: {output_path}")
        print(f"Using configuration path: {configuration_file_path}")
        proc = subprocess.Popen([
            f"{wayang_runner_dir}/wayang-submit",
            f"-Dwayang.configuration=file://{configuration_file_path}",
            f"org.apache.wayang.api.json.Main", 
            f"8080"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        time.sleep(5)
        try:
            print(f"Running process: {proc.pid} with args: {proc.args}")
            ctx = WayangContext() \
                .register({JavaPlugin, SparkPlugin})
            left = ctx.textfile(f"file://{resource_path}") \
                .filter(lambda w: "Apache" in w, str) \
                .flatmap(lambda w: w.split(), str, str) \
                .map(lambda w: (str(len(w)), w), str, (int, str))
            right = ctx.textfile(f"file://{resource_path}") \
                .filter(lambda w: "Wayang" in w, str) \
                .map(lambda w: (str(len(w)), w), str, (int, str))
            join = left.join(lambda w: w[0], right, lambda w: w[0], (int, str)) \
                .store_textfile(f"file://{output_path}")
            time.sleep(3)

            for _ in range(1):
                print(proc.stdout.readline())
        finally:    
            proc.kill()
