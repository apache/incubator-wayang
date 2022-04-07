#!/bin/bash

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

