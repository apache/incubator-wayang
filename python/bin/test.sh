#!/bin/bash

BASE=$(cd "$(dirname "$0")/.." | pwd)
cd ${BASE}

python -m unittest discover -s ./src/ --pattern=*test.py