# Compiling Apache Wayang

Apache Wayang has different dependencies, for compiling, it needs to add some profile in the compilation to enable maven works properly.

 ```shell
mvn clean compile -P scala-11,scala,antlr
```

The line before is because the plugin the Antlr is not needed in all the modules, as well it has happened with Scala language.

When maven compiles one or more modules using those plugins in the compilation time, it needs to add.

The modules are:
- wayang-api-scala-java_#Scala_Version#
- wayang-core <- Antlr
- wayang-iejoin_#Scala_Version#
- wayang-spark_#Scala_Version#
- wayang-profiler_#Scala_Version#
- wayang-tests-integration_#Scala_Version# 