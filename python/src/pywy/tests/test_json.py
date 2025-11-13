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

from importlib import resources
import json
import os
import subprocess
import time
import requests
from pywy.tests import resources as resources_folder

def test_json():
    # Specify the API URL we want to send our JSON to
    url = 'http://localhost:8080/wayang-api-json/submit-plan'
    # Specify the appropriate header for the POST request
    headers = {'Content-type': 'application/json'}

    print("Opening subprocess")
    try:
        with resources.path(resources_folder, "plan-a.json") as resource_path, \
            resource_path.open() as resource, \
            resources.path(resources_folder, "wayang.properties") as configuration_file_path: 
                proc = subprocess.Popen([
                    f"mvn", f"-q", f"-f", f"wayang-api/wayang-api-json/pom.xml", f"exec:java",
                    f"-Dexec.mainClass=org.apache.wayang.api.json.Main", 
                    f"-Dwayang.configuration=file://{configuration_file_path}", 
                    f"-Dexec.args=8080"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                
                for i in range(2):
                     print("[process.stdout]: ", proc.stdout.readline()), time.sleep(5) # wait for zio

                plan = json.load(resource)


                response = requests.post(url, headers=headers, json=plan)
                print(response)
    finally:
        proc.kill()