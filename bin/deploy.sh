#!/bin/bash

basedir="$(cd "$(dirname "$0")/.."; pwd)"
if [ "$(pwd)" != "$basedir" ]; then
	echo "Switching to $basedir"
	cd "$basedir"
fi

phase=verify
if [ "$1" == -f ]; then
	echo "Going for actual deployment."
	phase=deploy
else
	echo "Performing dry-run deployment."
	echo "Use $0 -f to do an actual deployment."
fi

scala_versions=(2.10.6 2.11.11) # 2.12.2 was not supported as of creating this script
failures=()
successes=()

for scala_version in "${scala_versions[@]}"; do
	"$basedir/bin/change-scala-version.sh" "$scala_version"
	if mvn clean "$phase" -Pdeployment; then
		successes+=("$scala_version")
	else
		failures+=("$scala_version")
	fi
done

echo "Summary:"
echo "* Successes with Scala versions ${successes[@]}"
echo "* Failures with Scala versions ${failures[@]}"

exit ${#failures[@]}
