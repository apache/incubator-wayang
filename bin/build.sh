#!/bin/bash

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


scala_versions=(2.11.8) # 2.12.2 was not supported as of creating this script
failures=()
successes=()

for scala_version in "${scala_versions[@]}"; do
	echo "Running 'mvn ${mvn_opts[@]}' with Scala $scala_version..."

	"$basedir/bin/change-scala-version.sh" "$scala_version"
	if mvn "${mvn_opts[@]}"; then
		successes+=("$scala_version")
	else
		failures+=("$scala_version")
	fi
done

echo "Summary:"
echo "* Successes with Scala versions ${successes[@]}"
echo "* Failures with Scala versions ${failures[@]}"

exit ${#failures[@]}
