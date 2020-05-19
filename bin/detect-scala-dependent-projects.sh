#!/bin/bash

echo "This scripts heuristically determines all (sub-)projects that seem to depend on Scala-dependencies and should therefore be sensitive w.r.t. the Scala version."

basedir="$(cd "$(dirname "$0")/.."; pwd)"
find "$basedir" -name pom.xml -a -type f -exec "$basedir/bin/contains-scala-dependencies.sh" "{}" \;
