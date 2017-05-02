# Rheem Benchmarks <img align="right" width="128px" src="http://da.qcri.org/rheem/images/rheem.png" alt="Rheem logo">

[![Gitter chat](https://badges.gitter.im/rheem-ecosystem/Lobby.png)](https://gitter.im/rheem-ecosystem/Lobby)

This repository provides example applications and further benchmarking tools to evaluate and get started with [Rheem](http://da.qcri.org/rheem/). More information is to come soon...

## Rheem applications

### WordCount

**Description.** This app takes a text input file and counts the number occurrences of each word in the text. This simple app has become some sort of _"Hello World"_ program for data processing systems.

**Running the app.** To run the app, launch the main class:
```java
org.qcri.rheem.apps.wordcount.WordCountScala
```
Even though this app is written in Scala, you can launch it in a regular JVM. Run the app without parameters to get a description of the required parameters.

**Datasets.** Find below a list of datasets that can be used to benchmark Rheem in combination with this app:
* [DBpedia - Long abstracts](http://wiki.dbpedia.org/Downloads2015-10) _NB: Consider stripping of the RDF container around the abstracts. It's not necessary, though._
