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

# create link file but also added in the git context
gln() {
    ln -s $1 $2
    git ls-files -s $2
    git add $2
}

addREADME(){
  echo "[comment]: # (Licensed to the Apache Software Foundation (ASF) under one or more)" > $1
  echo "[comment]: # (contributor license agreements.  See the NOTICE file distributed with)" >> $1
  echo "[comment]: # (this work for additional information regarding copyright ownership.)" >> $1
  echo "[comment]: # (The ASF licenses this file to You under the Apache License, Version 2.0)" >> $1
  echo "[comment]: # ((the \"License\"); you may not use this file except in compliance with)" >> $1
  echo "[comment]: # (the License.  You may obtain a copy of the License at)" >> $1
  echo "[comment]: # ()" >> $1
  echo "[comment]: # (http://www.apache.org/licenses/LICENSE-2.0)" >> $1
  echo "[comment]: # ()" >> $1
  echo "[comment]: # (Unless required by applicable law or agreed to in writing, software)" >> $1
  echo "[comment]: # (distributed under the License is distributed on an \"AS IS\" BASIS,)" >> $1
  echo "[comment]: # (WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.)" >> $1
  echo "[comment]: # (See the License for the specific language governing permissions and)" >> $1
  echo "[comment]: # (limitations under the License.)" >> $1
  echo "" >> $1
  if [ "${2}" == "scala" ]; then
    echo "In this folder need to be added all the code for scala ${3}" >> $1
  else
    echo "In this folder need to be added all the ${2} for scala ${3}" >> $1
  fi

}

REPOSITORY_PATH=$1
echo "${REPOSITORY_PATH}"
REPOSITORY_NAME=$(basename ${REPOSITORY_PATH})
SCALA_VERSION=(2.11 2.12)
FOLDER_TO_LINK=("java" "resources" "scala")

cd ${REPOSITORY_PATH}
pwd
git mv ./src ./code
echo "${REPOSITORY_NAME}"

for sc_version in ${SCALA_VERSION[@]} ;
do
  if [ -L "./${REPOSITORY_NAME}_${sc_version}/src" ]; then
    echo "deleting link"
    git rm -f ${REPOSITORY_NAME}_${sc_version}/src
    git rm -f ${REPOSITORY_NAME}_${sc_version}/scala_*
  fi
  mkdir -p ${REPOSITORY_NAME}_${sc_version}/src/{main,test}/{scala_${sc_version},resources_${sc_version}}
  for new_fol in $(find ${REPOSITORY_NAME}_${sc_version} -type d -empty)
  do
     type=$(basename ${new_fol} | tr '_' "\n")
     echo "${type}"
     addREADME ${new_fol}/README.md ${type}
     git add ${new_fol}/README.md
  done

  for fol in "${FOLDER_TO_LINK[@]}"
  do
    if [ -d "./code/main/${fol}" ]; then
      gln ./../../../code/main/${fol} ./${REPOSITORY_NAME}_${sc_version}/src/main/${fol}
    fi
    if [ -d "./code/test/${fol}" ]; then
      gln ./../../../code/test/${fol} ./${REPOSITORY_NAME}_${sc_version}/src/test/${fol}
    fi
  done

done
