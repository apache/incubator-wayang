---
license: |
    Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    (the "License"); you may not use this file except in compliance with
    the License.  You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
layout: default
title: "Read Me"
menus: header
previous:
    url: /
    title: previous
next:
    url: /
    title: next
menus:
    header:
        weight: 0
---

# Apache Wayang (incubating) <img align="right" style="margin-top: -0.5em;" width="170px" src="https://wayang.apache.org/assets/img/logo/logo_400x160.png" alt="Wayang logo">

[![Build Status (Travis)](https://travis-ci.org/wayang-ecosystem/wayang.svg?branch=master)](https://travis-ci.org/wayang-ecosystem/wayang)
[![Gitter chat](https://badges.gitter.im/wayang-ecosystem/Lobby.png)](https://gitter.im/wayang-ecosystem/Lobby)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.apache.wayang/wayang/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.apache.wayang/wayang)

#### Apache Wayang (incubating) - A Federated Data Processing Engine

Unlike conventional data processing systems that depend on a single execution engine, Apache Wayang (incubating) acts as a meta processing framework. It empowers you to specify your data processing application through one of its APIs, and Wayang then intelligently chooses the ideal combination of underlying processing frameworks, like Java Streams or Apache Spark, to run your application efficiently. Wayang seamlessly manages inter-platform communication, eliminating the need to grapple with various platform APIs.
   
Wayang has built in support for the following frameworks:

- Apache Flink v1.7.1
- Apache Giraph v1.2.0-hadoop2
- GraphChi v0.2.2 (only available with scala 11.x)
- Java Streams (version depends on the java version)
- JDBC-Template
- Postgres v9.4.1208 (Implementation JDBC-Template)
- Apache Spark v3.1.2 (scala 12.x) and v2.4.8 (scala 11.x)
- SQLite3 v3.8.11.2 (implementation JDBC-Template)

Important note: depending on the scala version the list of the supported platforms available could be different.

## How to use Wayang

**Requirements.**
Apache Wayang (incubating) is built upon the foundations of Java 11 and Scala 2.12, providing a robust and versatile platform for data processing applications. If you intend to build Wayang from source, you will also need to have Apache Maven, the popular build automation tool, installed on your system. Additionally, be mindful that some of the processing platforms supported by Wayang may have their own specific installation requirements.

**Get Wayang.**
Wayang is available via Maven Central. To use it with Maven, for instance, include the following into you POM file:
```xml
<dependency>
  <groupId>org.apache.wayang</groupId>
  <artifactId>wayang-***</artifactId>
  <version>0.7.1</version>
</dependency>
```
Note the `***`: Wayang ships with multiple modules that can be included in your app, depending on how you want to use it:
* `wayang-core`: provides core data structures and the optimizer (required)
* `wayang-basic`: provides common operators and data types for your apps (recommended)
* `wayang-api`: provides an easy-to-use Scala and Java API to assemble Wayang plans (recommended)
* `wayang-java`, `wayang-spark`, `wayang-graphchi`, `wayang-sqlite3`, `wayang-postgres`: adapters for the various supported processing platforms
* `wayang-profiler`: provides functionality to learn operator and UDF cost functions from historical execution data

For the sake of version flexibility, you still have to include your Hadoop (`hadoop-hdfs` and `hadoop-common`) and Spark (`spark-core` and `spark-graphx`) version of choice.

In addition, you can obtain the most recent snapshot version of Wayang via Sonatype's snapshot repository. Just included
```xml
<repositories>
  <repository>
    <id>sonatype-snapshots</id>
    <name>Sonatype Snapshot Repository</name>
    <url>https://oss.sonatype.org/content/repositories/snapshots</url>
  </repository>
<repositories>
```

If you need to rebuild Wayang, e.g., to use a different Scala version, you can simply do so via Maven:

1. Adapt the version variables (e.g., `spark.version`) in the main `pom.xml` file.
2. Build Wayang with the adapted versions.
   ```shell
   $ mvn clean install
   ```
   Note the `standalone` profile to fix Hadoop and Spark versions, so that Wayang apps do not explicitly need to declare the corresponding dependencies.
   Also, note the `distro` profile, which assembles a binary Wayang distribution.
   To activate these profiles, you need to specify them when running maven, i.e.,
    ```shell
    mvn clean install -P<profile name>
    ```

**Configure Wayang.** To enable Apache Wayang's smooth operation, you need to equip it with details about your processing platforms' capabilities and how to interact with them. A default configuration is available for initial testing, but creating a properties file is generally preferable for fine-tuning the configuration to suit your specific requirements. To harness this personalized configuration effortlessly, launch your application via
```shell
$ java -Dwayang.configuration=url://to/my/wayang.properties ...
```

Essential configuration settings:
* General settings
    * `wayang.core.log.enabled (= true)`: whether to log execution statistics to allow learning better cardinality and cost estimators for the optimizer
    * `wayang.core.log.executions (= ~/.wayang/executions.json)` where to log execution times of operator groups
    * `wayang.core.log.cardinalities (= ~/.wayang/cardinalities.json)` where to log cardinality measurements
    * `wayang.core.optimizer.instrumentation (= org.apache.wayang.core.profiling.OutboundInstrumentationStrategy)`: where to measure cardinalities in Wayang plans; other options are `org.apache.wayang.core.profiling.NoInstrumentationStrategy` and `org.apache.wayang.core.profiling.FullInstrumentationStrategy`
    * `wayang.core.optimizer.reoptimize (= false)`: whether to progressively optimize Wayang plans
    * `wayang.basic.tempdir (= file:///tmp)`: where to store temporary files, in particular for inter-platform communication
* Java Streams
    * `wayang.java.cpu.mhz (= 2700)`: clock frequency of processor the JVM runs on in MHz
    * `wayang.java.hdfs.ms-per-mb (= 2.7)`: average throughput from HDFS to JVM in ms/MB
* Apache Spark
    * `spark.master (= local)`: Spark master
        * various other Spark settings are supported, e.g., `spark.executor.memory`, `spark.serializer`, ...
    * `wayang.spark.cpu.mhz (= 2700)`: clock frequency of processor the Spark workers run on in MHz
    * `wayang.spark.hdfs.ms-per-mb (= 2.7)`: average throughput from HDFS to the Spark workers in ms/MB
    * `wayang.spark.network.ms-per-mb (= 8.6)`: average network throughput of the Spark workers in ms/MB
    * `wayang.spark.init.ms (= 4500)`: time it takes Spark to initialize in ms
* GraphChi
    * `wayang.graphchi.cpu.mhz (= 2700)`: clock frequency of processor GraphChi runs on in MHz
    * `wayang.graphchi.cpu.cores (= 2)`: number of cores GraphChi runs on
    * `wayang.graphchi.hdfs.ms-per-mb (= 2.7)`: average throughput from HDFS to GraphChi in ms/MB
* SQLite
    * `wayang.sqlite3.jdbc.url`: JDBC URL to use SQLite
    * `wayang.sqlite3.jdbc.user`: optional user name
    * `wayang.sqlite3.jdbc.password`: optional password
    * `wayang.sqlite3.cpu.mhz (= 2700)`: clock frequency of processor SQLite runs on in MHz
    * `wayang.sqlite3.cpu.cores (= 2)`: number of cores SQLite runs on
* PostgreSQL
    * `wayang.postgres.jdbc.url`: JDBC URL to use PostgreSQL
    * `wayang.postgres.jdbc.user`: optional user name
    * `wayang.postgres.jdbc.password`: optional password
    * `wayang.postgres.cpu.mhz (= 2700)`: clock frequency of processor PostgreSQL runs on in MHz
    * `wayang.postgres.cpu.cores (= 2)`: number of cores PostgreSQL runs on

**Code with Wayang.** To effectively define your applications with Apache Wayang, utilize its Scala or Java API, conveniently found within the `wayang-api` module. For clear illustrations, refer to the provided examples below.

**Learn cost functions.**
Wayang provides a utility to learn cost functions from historical execution data. Specifically, Wayang can learn configurations for load profile estimators (that estimate CPU load, disk load etc.) for both operators and UDFs, as long as the configuration provides a template for those estimators.
   
As an example, the `JavaMapOperator` draws its load profile estimator configuration via the configuration key `wayang.java.map.load`.
Now, it is possible to specify a load profile estimator template in the configuration under the key `<original key>.template`, e.g.:
```xml
wayang.java.map.load.template = {\
  "in":1, "out":1,\
  "cpu":"?*in0"\
}
```
This template encapsulates a load profile estimator that requires at minimum one input cardinality and one output cardinality. Furthermore, it simulates CPU load by assuming a direct relationship with the input cardinality. However, more complex functions are possible.
   
In particular, you can use
* the variables `in0`, `in1`, ... and `out0`, `out1`, ... to incorporate the input and output cardinalities, respectively;
* operator properties, such as `numIterations` for the `PageRankOperator` implementations;
* the operators `+`, `-`, `*`, `/`, `%`, `^`, and parantheses;
* the functions `min(x0, x1, ...))`, `max(x0, x1, ...)`, `abs(x)`, `log(x, base)`, `ln(x)`, `ld(x)`;
* and the constants `e` and `pi`.

While Apache Wayang provides templates for all execution operators, you will need to explicitly define your user-defined functions (UDFs) by specifying their cost functions, which are based on configuration parameters. This involves creating an initial specification and template for each UDF.
As soon as execution data has been collected, you can initiate:
```shell
java ... org.apache.wayang.profiler.ga.GeneticOptimizerApp [configuration URL [execution log]]
```
This tool will attempt to determine suitable values for the question marks (`?`) within the load profile estimator templates, aligning them with the collected execution data and pre-defined configuration entries for the load profile estimators. These optimized values can then be directly incorporated into your configuration.

## Examples

For some executable examples, have a look at [this repository](https://github.com/sekruse/rheem-examples).

### WordCount

#### Java API
```java
import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.apache.wayang.java.Java;
import org.apache.wayang.spark.Spark;
import java.util.Collection;
import java.util.Arrays;

public class WordcountJava {

    public static void main(String[] args){

        // Settings
        String inputUrl = "file:/tmp.txt";

        // Get a plan builder.
        WayangContext wayangContext = new WayangContext(new Configuration())
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin());
        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName(String.format("WordCount (%s)", inputUrl))
                .withUdfJarOf(WordcountJava.class);

        // Start building the WayangPlan.
        Collection<Tuple2<String, Integer>> wordcounts = planBuilder
                // Read the text file.
                .readTextFile(inputUrl).withName("Load file")

                // Split each line by non-word characters.
                .flatMap(line -> Arrays.asList(line.split("\\W+")))
                .withSelectivity(10, 100, 0.9)
                .withName("Split words")

                // Filter empty tokens.
                .filter(token -> !token.isEmpty())
                .withSelectivity(0.99, 0.99, 0.99)
                .withName("Filter empty words")

                // Attach counter to each word.
                .map(word -> new Tuple2<>(word.toLowerCase(), 1)).withName("To lower case, add counter")

                // Sum up counters for every word.
                .reduceByKey(
                        Tuple2::getField0,
                        (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1())
                )
                .withCardinalityEstimator(new DefaultCardinalityEstimator(0.9, 1, false, in -> Math.round(0.01 * in[0])))
                .withName("Add counters")

                // Execute the plan and collect the results.
                .collect();

        System.out.println(wordcounts);
    }
}
```

#### Scala API

```scala
import org.apache.wayang.api._
import org.apache.wayang.core.api.{Configuration, WayangContext}
import org.apache.wayang.java.Java
import org.apache.wayang.spark.Spark

object WordcountScala {
  def main(args: Array[String]) {

    // Settings
    val inputUrl = "file:/tmp.txt"

    // Get a plan builder.
    val wayangContext = new WayangContext(new Configuration)
      .withPlugin(Java.basicPlugin)
      .withPlugin(Spark.basicPlugin)
    val planBuilder = new PlanBuilder(wayangContext)
      .withJobName(s"WordCount ($inputUrl)")
      .withUdfJarsOf(this.getClass)

    val wordcounts = planBuilder
      // Read the text file.
      .readTextFile(inputUrl).withName("Load file")

      // Split each line by non-word characters.
      .flatMap(_.split("\\W+"), selectivity = 10).withName("Split words")

      // Filter empty tokens.
      .filter(_.nonEmpty, selectivity = 0.99).withName("Filter empty words")

      // Attach counter to each word.
      .map(word => (word.toLowerCase, 1)).withName("To lower case, add counter")

      // Sum up counters for every word.
      .reduceByKey(_._1, (c1, c2) => (c1._1, c1._2 + c2._2)).withName("Add counters")
      .withCardinalityEstimator((in: Long) => math.round(in * 0.01))

      // Execute the plan and collect the results.
      .collect()

    println(wordcounts)
  }
}
```

### k-means

Wayang is also capable of iterative processing, which is, e.g., very important for machine learning algorithms, such as k-means.

#### Scala API

```scala
import org.apache.wayang.api._
import org.apache.wayang.core.api.{Configuration, WayangContext}
import org.apache.wayang.core.function.FunctionDescriptor.ExtendedSerializableFunction
import org.apache.wayang.core.function.ExecutionContext
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimators
import org.apache.wayang.java.Java
import org.apache.wayang.spark.Spark

import scala.util.Random
import scala.collection.JavaConversions._

object kmeans {
  def main(args: Array[String]) {

    // Settings
    val inputUrl = "file:/kmeans.txt"
    val k = 5
    val iterations = 100
    val configuration = new Configuration

    // Get a plan builder.
    val wayangContext = new WayangContext(new Configuration)
      .withPlugin(Java.basicPlugin)
      .withPlugin(Spark.basicPlugin)
    val planBuilder = new PlanBuilder(wayangContext)
      .withJobName(s"k-means ($inputUrl, k=$k, $iterations iterations)")
      .withUdfJarsOf(this.getClass)

    case class Point(x: Double, y: Double)
    case class TaggedPoint(x: Double, y: Double, cluster: Int)
    case class TaggedPointCounter(x: Double, y: Double, cluster: Int, count: Long) {
      def add_points(that: TaggedPointCounter) = TaggedPointCounter(this.x + that.x, this.y + that.y, this.cluster, this.count + that.count)
      def average = TaggedPointCounter(x / count, y / count, cluster, 0)
    }

    // Read and parse the input file(s).
    val points = planBuilder
      .readTextFile(inputUrl).withName("Read file")
      .map { line =>
        val fields = line.split(",")
        Point(fields(0).toDouble, fields(1).toDouble)
      }.withName("Create points")


    // Create initial centroids.
    val random = new Random
    val initialCentroids = planBuilder
      .loadCollection(for (i <- 1 to k) yield TaggedPointCounter(random.nextGaussian(), random.nextGaussian(), i, 0)).withName("Load random centroids")

    // Declare UDF to select centroid for each data point.
    class SelectNearestCentroid extends ExtendedSerializableFunction[Point, TaggedPointCounter] {

      /** Keeps the broadcasted centroids. */
      var centroids: Iterable[TaggedPointCounter] = _

      override def open(executionCtx: ExecutionContext) = {
        centroids = executionCtx.getBroadcast[TaggedPointCounter]("centroids")
      }

      override def apply(point: Point): TaggedPointCounter = {
        var minDistance = Double.PositiveInfinity
        var nearestCentroidId = -1
        for (centroid <- centroids) {
          val distance = Math.pow(Math.pow(point.x - centroid.x, 2) + Math.pow(point.y - centroid.y, 2), 0.5)
          if (distance < minDistance) {
            minDistance = distance
            nearestCentroidId = centroid.cluster
          }
        }
        new TaggedPointCounter(point.x, point.y, nearestCentroidId, 1)
      }
    }

    // Do the k-means loop.
    val finalCentroids = initialCentroids.repeat(iterations, { currentCentroids =>
      points
        .mapJava(new SelectNearestCentroid,
          udfLoad = LoadProfileEstimators.createFromSpecification(
            "my.udf.costfunction.key", configuration
          ))
        .withBroadcast(currentCentroids, "centroids").withName("Find nearest centroid")
        .reduceByKey(_.cluster, _.add_points(_)).withName("Add up points")
        .withCardinalityEstimator(k)
        .map(_.average).withName("Average points")
    }).withName("Loop")

      // Collect the results.
      .collect()

    println(finalCentroids)
  }
}
```

## License

All files in this repository are licensed under the Apache Software License 2.0

Copyright 2020 - 2024 The Apache Software Foundation.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
