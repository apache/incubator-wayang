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

basedir="$(cd "$(dirname "$0")/.."; pwd)"

if [ $# -eq 0 ]; then
	echo "Performing dry-run deployment."
	mvn_opts=(clean verify -Pdeployment-dryrun)
	echo "Other options:"
	echo " $0 -f do an actual deployment."
	echo " $0 -e <mvn args>... run exactly the given Maven args."
elif [ "$1" == -f ]; then
	echo "Going for actual deployment."
	mvn_opts=(clean deploy -Pdeployment)
elif [ "$1" == -e ]; then
	shift
	mvn_opts=("$@")
	echo "Running mvn ${mvn_opts[@]}"
else
	echo "Invalid option. Other options:"
	echo " $0 do a deployment dry-run."
	echo " $0 -f do an actual deployment."
	echo " $0 -e <mvn args>... run exactly the given Maven args."
	exit 1
fi

if [ "$(pwd)" != "$basedir" ]; then
	echo "Switching to $basedir"
	cd "$basedir"
fi


scala_versions=(scala-11 scala-12)
failures=()
successes=()

for scala_version in "${scala_versions[@]}"; do
	echo "Running 'mvn ${mvn_opts[@]}' with Scala $scala_version..."

	if mvn "${mvn_opts[@]}" -P $scala_version; then
		successes+=("$scala_version")
	else
		failures+=("$scala_version")
	fi
done

echo "Summary:"
echo "* Successes with Scala versions: ${successes[@]}"
echo "* Failures with Scala versions: ${failures[@]}"

exit ${#failures[@]}
