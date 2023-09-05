<!---
  Licensed to the Apache Software Foundation (ASF) under one or more
  license agreements; and to You under the Apache License, version 2.0:

    https://www.apache.org/licenses/LICENSE-2.0

  This file is part of the Apache Wayang (incubating) project.
--->

# Wayang Assembly
This is an assembly module for Apache Wayang(Incubator) project.

It creates a single tar.gz file that includes all needed dependency of the project
except for the jars in the list

- org.apache.hadoop.*, those are supposed to be available from the deployed Hadoop cluster.

> Note: This module is off by default. To activate it specify the profile in the command line
-Pdistribution

> Note: If you need to build an assembly for a different version of Hadoop the
> hadoop-version system property needs to be set as in this example: `-Dhadoop.version=2.7.4` at the 
> maven command line


# Execution Profile Assembly

To execute the Wayang Assembly you need to execute the following command in the project root

```shell
./mvnw clean install -DskipTests 
./mvnw clean package -pl :wayang-assembly -Pdistribution
```
