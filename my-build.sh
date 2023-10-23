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

rm -r wayang-0.6.1-SNAPSHOT
set -e
mvn -T 1C -U clean install -Dmaven.test.skip
# mvn clean package -DskipTests -pl :wayang-benchmark
mvn clean package -pl :wayang-assembly -Pdistribution
tar -xvf wayang-assembly/target/wayang-assembly-0.6.1-SNAPSHOT-dist.tar.gz

# cd wayang-0.6.1-SNAPSHOT
# echo "export WAYANG_HOME=$(pwd)" >> ~/.bashrc
# echo "export PATH=${PATH}:${WAYANG_HOME}/bin" >> ~/.bashrc
# source ~/.bashrc

# ./bin/wayang-submit org.apache.wayang.apps.wordcount.Main java file://$(pwd)/README.md

# mvn test -Dtest=org.apache.wayang.jdbc.operators.SqlToRddOperatorTest -pl wayang-platforms/wayang-jdbc-template