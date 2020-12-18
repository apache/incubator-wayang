#!/bin/bash

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
