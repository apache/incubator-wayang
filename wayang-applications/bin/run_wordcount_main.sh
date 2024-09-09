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

cd ../..

export JAVA_HOME=/Users/kamir/.sdkman/candidates/java/current
export SPARK_HOME=/opt/homebrew/Cellar/apache-spark/3.5.2/libexec
export HADOOP_HOME=/opt/homebrew/Cellar/hadoop/3.4.0/libexec
export PATH=$PATH:$HADOOP_HOME/bin
export WAYANG_HOME="/Users/kamir/GITHUB.merge/incubator-wayang/wayang-assembly/target/apache-wayang-assembly-0.7.1-incubating-dist/wayang-0.7.1"
export WAYANG_APP_HOME="/Users/kamir/GITHUB.merge/incubator-wayang/wayang-applications/target/*"

# mvn clean compile package install -pl :wayang-assembly -Pdistribution -DskipTests

bin/wayang-submit org.apache.wayang.apps.wordcount.Main java file://$(pwd)/README.md

