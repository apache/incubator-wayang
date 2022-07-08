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

function rename_structure {
  pattern="s#/$1\$##"
  for f in $(ls -R ./ | grep ":$" | sed "s/:$//" | grep "$1$" | sed $pattern ) ; do
     git mv "$f/$2" "$f/$3"
  done
}

function change_files {
  find . -not -path '*/\.*' -type f -exec sed -i '' "s/$1/$2/g" {} \;
}

cd ../

#TODO replace new_one with the new
rename_structure io io org
rename_structure wayang/wayang wayang  apache

change_files "io\.wayang\.wayang" "new_one\.new_one\.wayang"
