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

# Define some colors
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

# Array of Main classes
java_mains=(
    "org.apache.wayang.async.apps.WordCount"
    "org.apache.wayang.multicontext.apps.wordcount.WordCount"
    "org.apache.wayang.multicontext.apps.wordcount.WordCountCombineEach"
    "org.apache.wayang.multicontext.apps.wordcount.WordCountWithMerge"
    "org.apache.wayang.multicontext.apps.wordcount.WordCountWithTargetPlatforms"
)

# Cleanup command
cleanup_command="rm -r /tmp/out*"

# Path to java executable
java_path="./bin/wayang-submit"

# Iterate over Main classes
for main_class in "${java_mains[@]}"
do
  echo -e "${GREEN}Running $main_class...${NC}"

  # Run Java Main class
  "$java_path" "$main_class"

  # Check if Java program ran successfully
  if [ $? -ne 0 ]; then
    echo -e "${RED}Java main class $main_class failed. Exiting...${NC}"
    exit 1
  fi

  echo -e "${GREEN}Cleaning up...${NC}"

  # Run cleanup command
  eval "$cleanup_command"

  # Check if cleanup succeeded
  if [ $? -ne 0 ]; then
    echo -e "${RED}Cleanup failed. Exiting...${NC}"
    exit 1
  fi

done

echo -e "${GREEN}All Java main programs have been executed and cleaned successfully.${NC}"