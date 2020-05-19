#!/bin/bash

basedir="$(cd "$(dirname "$0")/.."; pwd)"

find "$basedir" -name "pom.xml.*" -exec rm {} \;
