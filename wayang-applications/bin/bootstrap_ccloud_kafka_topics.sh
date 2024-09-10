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

#brew install confluentinc/tap/cli
#brew install jq
#brew install git-lfs

source .env.sh
source env.demo1.sh

confluent kafka topic delete $topic_l1_a --cluster $DEMO1_CLUSTER1
confluent kafka topic delete $topic_l1_b --cluster $DEMO1_CLUSTER1
confluent kafka topic delete $topic_l1_c --cluster $DEMO1_CLUSTER1
confluent kafka topic delete $topic_l2_a --cluster $DEMO1_CLUSTER1
confluent kafka topic delete $topic_l2_b --cluster $DEMO1_CLUSTER1

confluent kafka topic create $topic_l1_a --cluster $DEMO1_CLUSTER1
confluent kafka topic create $topic_l1_b --cluster $DEMO1_CLUSTER1
confluent kafka topic create $topic_l1_c --cluster $DEMO1_CLUSTER1
confluent kafka topic create $topic_l2_a --cluster $DEMO1_CLUSTER1
confluent kafka topic create $topic_l2_b --cluster $DEMO1_CLUSTER1

confluent kafka topic list --cluster $DEMO1_CLUSTER1

curl --silent -X GET -u $SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO $SCHEMA_REGISTRY_URL/subjects | jq .
