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

# THIS SCRIPT IS DEPRECATED since https://github.com/apache/incubator-wayang/pull/26

if [ $# -eq 0 ]; then
	>&2 echo "No parameters given."
	echo "This script changes the Scala version used in this project."
	echo "Usage: $0 <new Scala version (e.g., 2.12.4)>"
	exit 1
fi

basedir="$(cd "$(dirname "$0")/.."; pwd)"

# Detect the old versions.
old_minor="$(grep -o "<scala.version>.*</scala.version>" "$basedir/pom.xml" | sed 's/^<scala.version>\(.*\)<\/scala.version>$/\1/')"
old_major="$(grep -o "<scala.compat.version>.*</scala.compat.version>" "$basedir/pom.xml" | sed 's/^<scala.compat.version>\(.*\)<\/scala.compat.version>$/\1/')"
echo "Old Scala version: $old_major/$old_minor"

new_minor="$1"
new_major="$(echo "$new_minor" | awk -F'.' '{printf("%s.%s", $1, $2);}')"

echo "New Scala version: $new_major/$new_minor"

case "$(uname -s)" in
	Darwin)
		sed_opts=("-i" ".bak")
		;;
	Linux)
		sed_opts=("-i.bak")
		;;
	*)
		echo "Unknown OS... configure this script to get the sed command right."
		exit 2
esac

echo "Applying changes..."
find "$basedir" -name pom.xml -a -type f -exec \
	sed "${sed_opts[@]}" -e "s/<scala.version>$old_minor<\/scala.version>/<scala.version>$new_minor<\/scala.version>/" \
	-e "s/<scala.compat.version>$old_major<\/scala.compat.version>/<scala.compat.version>$new_major<\/scala.compat.version>/" \
	-e "s/_$old_major<\//_$new_major<\//" \
	{} ';'
