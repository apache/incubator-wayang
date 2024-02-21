# Pywayang notes

## Prerequisites
Users need to install further software:
- Protobuf (protoc)
- Python (obviously)
- Protoc script to create protobuf in python needs to be called on build
  of python library

## Limitations
- Current structure is counter-intuitive (not in wayang-api-python)
- Flatmap operators don't seem to work (minor problem)
- Few platforms / operators are supported in the current state
- Manual start of REST API needed
- Operators are not platform agnostic in python

## Benefits
- Concise and easy way to derive WayangPlans in Python
- If setup of library is easy, pulling Wayang as 3rd party and starting
API is all the setup needed to get going

## TODO
- Add README.MD
- Add Tests
- Make platform independent
- Automate setup (pip install of dist)


