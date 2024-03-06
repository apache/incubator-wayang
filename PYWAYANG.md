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

## Benefits
- Concise and easy way to derive WayangPlans in Python
- If setup of library is easy, pulling Wayang as 3rd party and starting
API is all the setup needed to get going

## Call-to-action
- Remove pywy.core.Translator that is used to translate a perfectly fine
  platform independent plan into plugin specific execution operators
(somehow always takes the first plugin specified).
- Ideally, use Scala API so that we don't need to have multiple versions
  of a REST API.
- Remove Protobuf auto generation and just transform the python plan and
  operators to the data types that are defined in the scala REST API (if
  not possible with JPype, do it manually).
- Start the Scala REST API as a stop of invoking PywyPlan.execute() in
  Python API using JPype so that users won't have to start the Wayang
  REST API in a different process.


