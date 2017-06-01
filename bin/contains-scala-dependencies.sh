#!/bin/bash

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
