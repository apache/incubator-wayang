#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

######
# to adjust variables to your own environment, please configure them in env.sh
#
source .env.sh

cd ..
cd ..

mvn clean compile package install -pl :wayang-assembly -Pdistribution -DskipTests

cd wayang-applications
mvn compile package install -DskipTests

cd ..

source ./.env.sh; bin/wayang-submit org.apache.wayang.applications.WordCount java file://$(pwd)/wayang-applications/data/case-study/DATA_REPO_001/README.md
