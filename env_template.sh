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
export BOOTSTRAP_SERVER=...
export CLUSTER_API_KEY=...
export CLUSTER_API_SECRET=...
export SR_ENDPOINT=...
export SR_API_KEY=...
export SR_API_SECRET=...
export SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO="...:...."
export SCHEMA_REGISTRY_URL="https://...."

export SPARK_HOME=/opt/homebrew/Cellar/apache-spark/3.5.2/libexec
export HADOOP_HOME=/opt/homebrew/Cellar/hadoop/3.4.0/libexec
export PATH=$PATH:$HADOOP_HOME/bin
export WAYANG_HOME="/Users/kamir/GITHUB.merge/incubator-wayang/wayang-assembly/target/apache-wayang-assembly-0.7.1-incubating-dist/wayang-0.7.1"
export WAYANG_APP_HOME="/Users/kamir/GITHUB.merge/incubator-wayang/wayang-applications/target/*"

echo "Hadoop home : $HADOOP_HOME"
echo "Spark home  : $SPARK_HOME"
echo "Wayang home : $WAYANG_HOME"
echo "Wayang app  : $WAYANG_APP_HOME"


