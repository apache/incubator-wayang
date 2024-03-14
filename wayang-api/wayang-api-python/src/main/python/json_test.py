import subprocess
import json
import requests

# Specify the API URL we want to send our JSON to
url = 'http://localhost:8080/wayang-api-json/submit-plan/drawflow-format'
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
