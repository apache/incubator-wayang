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

verbose=false

if [ $# -ne 1 ]; then
	>&2 echo "$0 expects exactly one input path."
	exit 1
fi

if [ ! -f "$1" ] || [[ ! "$1" =~ /pom.xml$ ]]; then
	>&2 echo "$1 is not a pom.xml."
	exit 1
fi

scaladeps=$(cd "$(dirname "$1")" && mvn -o dependency:list | grep -e "_2.[891][0-9]*:" | wc -l | tr -cd "[0-9]")
echo "$1 has $scaladeps Scala dependencies."
[ "$verbose" == true ] && [ $scaladeps -gt 0 ] && (cd "$(dirname "$1")" && mvn -o dependency:list | grep -e "_2.[891][0-9]*:" | awk '{printf("- %s\n", $0)}')
