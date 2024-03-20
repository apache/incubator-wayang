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
import subprocess
import json
import requests

# Specify the API URL we want to send our JSON to
url = 'http://localhost:8080/wayang-api-json/submit-plan'
# Specify the appropriate header for the POST request
headers = {'Content-type': 'application/json'}

with open("/var/www/html/wayang-api/wayang-api-json/src/main/resources/plan-a.json") as f:
    plan = json.load(f)
    print(plan)

    #with subprocess.Popen(["/var/www/html/wayang-assembly/target/wayang-0.7.1/bin/wayang-submit org.apache.wayang.api.json.springboot.SpringBootApplication"], stdout=subprocess.PIPE, shell=True) as proc:
        #print("Started wayang-api-json")
        #print(proc.stdout.read())
    response = requests.post(url, headers=headers, json=plan)
    print(response)
        #print(proc.terminate())
