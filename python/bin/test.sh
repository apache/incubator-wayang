#!/bin/bash


cd "$(dirname "$0")/.."

python -m unittest discover -s ./src/ --pattern=*test.py