#!/bin/bash

################################################################################
##
##  Licensed to the Apache Software Foundation (ASF) under one or more
##  contributor license agreements.  See the NOTICE file distributed with
##  this work for additional information regarding copyright ownership.
##  The ASF licenses this file to You under the Apache License, Version 2.0
##  (the "License"); you may not use this file except in compliance with
##  the License.  You may obtain a copy of the License at
##
##      http://www.apache.org/licenses/LICENSE-2.0
##
##  Unless required by applicable law or agreed to in writing, software
##  distributed under the License is distributed on an "AS IS" BASIS,
##  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
##  See the License for the specific language governing permissions and
##  limitations under the License.
##
################################################################################

BASE=$(cd "$(dirname "$0")/.." | pwd)
echo "$BASE"

function create_next_file(){
  path=$1
  prev=$2
  curr=$((prev + 1))
  real_orig_path=$(echo "${path}" | sed "s/##/${prev}/g")
  real_dest_path=$(echo "${path}" | sed "s/##/${curr}/g")
  if [ -f "${real_dest_path}" ] ; then
    echo "skiping the generation of ${real_dest_path}, because exist"
    return
  fi
  if [ ! -f "${real_orig_path}" ] ; then
    echo "it is not possible to generate the file ${real_dest_path}, because does not exist ${real_orig_path}"
    return
  fi
  touch ${real_dest_path}
  for i in {1..10} ; do
    cat "${real_orig_path}" >> ${real_dest_path}
  done
}

# this will generate from 1MB until 100GB of data of text
for i in {0..4} ; do
  create_next_file "${BASE}/src/pywy/tests/resources/10e##MB.input" ${i}
done
ls -lah ${BASE}/src/pywy/tests/resources/ | grep "10e"

