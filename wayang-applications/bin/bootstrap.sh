
#brew install confluentinc/tap/cli
#brew install jq
#brew install git-lfs

export topic_l1_a=region_emea_counts
export topic_l1_b=region_apac_counts
export topic_l1_c=region_uswest_counts
export topic_l2_a=global_contribution
export topic_l2_b=global_averages

#confluent login
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


confluent kafka topic delete topic_l1_a --cluster lkc-m2kpj2
confluent kafka topic delete topic_l1_b --cluster lkc-m2kpj2
confluent kafka topic delete topic_l1_c --cluster lkc-m2kpj2
confluent kafka topic delete topic_l2_a --cluster lkc-m2kpj2
confluent kafka topic delete topic_l2_b --cluster lkc-m2kpj2

confluent kafka topic create topic_l1_a --cluster lkc-m2kpj2
confluent kafka topic create topic_l1_b --cluster lkc-m2kpj2
confluent kafka topic create topic_l1_c --cluster lkc-m2kpj2
confluent kafka topic create topic_l2_a --cluster lkc-m2kpj2
confluent kafka topic create topic_l2_b --cluster lkc-m2kpj2

######################################################################################################
# https://docs.confluent.io/cloud/current/sr/schema_registry_ccloud_tutorial.html
export SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO="ZZUZ3HASNNFGE2DF:EQNB0QpzDd868qlW0Nz49anodp7JjeDkoCaZelCJiUhfTX7BhuRPhNlDA/swx/Fa"
export SCHEMA_REGISTRY_URL="https://psrc-lo5k9.eu-central-1.aws.confluent.cloud"

confluent kafka topic list --cluster lkc-m2kpj2

###
# List schemas
curl --silent -X GET -u $SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO $SCHEMA_REGISTRY_URL/subjects | jq .
