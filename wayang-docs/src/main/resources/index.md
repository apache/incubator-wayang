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

#### Turning a shadows into a show


Apache Wayang (incubating) in contrast to classical data processing systems that provide one dedicated execution engine, Apache Wayang (incubating) rather is a *meta processing framework*: You can specify your data processing app via one of Wayang's API and then Wayang will pick an optimal configuration of classical processing frameworks, such as Java Streams or Apache Spark, to run your app on. Finally, Wayang will also perform the execution, thereby hiding the different specific platform APIs and coordinate inter-platform communication.

This approach aims at freeing data engineers and software developers from the burden of knowing the zoo of different data processing systems, their APIs, strengths and weakness; the intricacies of coordinating and integrating different processing platforms; and the inflexibility when tying to a fix set of processing platforms. As of now, Wayang has built in support for the following processing platforms:
- Java 8 Streams
- [Apache Spark](https://spark.apache.org/)
- [GraphChi](https://github.com/GraphChi/graphchi-java)
- [Postgres](http://www.postgresql.org)
- [SQLite](https://www.sqlite.org/)

## How to use Wayang

**Requirements.**
Apache Wayang (incubating) is built with Java 8 and Scala 2.11. However, to execute Wayang it is sufficient to have Java 8 installed. If you want to build Wayang yourself, you will also need to have [Apache Maven](http://maven.apache.org) installed. Please also consider that processing platforms employed by Wayang might have further requirements.

**Get Wayang.**
Wayang is available via Maven Central. To use it with Maven, for instance, include the following into you POM file:
```xml
<dependency> 
  <groupId>org.apache.wayang</groupId>
  <artifactId>wayang-***</artifactId>
  <version>0.3.0</version> 
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

**Configure Wayang.** In order for Wayang to work properly, it is necessary to tell Wayang about the capacities of your processing platforms and how to reach them. While there is a default configuration that allows to test Wayang right away, we recommend to create a properties file to adapt the configuration where necessary. To have Wayang use that configuration transparently, just run you app via
```shell
$ java -Dwayang.configuration=url://to/my/wayang.properties ...
```

You can find the most relevant settings in the following:
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

**Code with Wayang.** The recommended way to specify your apps with Wayang is via its Scala or Java API from the `wayang-api` module. You can find examples below.

**Learn cost functions.**
Wayang provides a utility to learn cost functions from historical execution data.
Specifically, Wayang can learn configurations for load profile estimators (that estimate CPU load, disk load etc.) for both operators and UDFs, as long as the configuration provides a template for those estimators.
As an example, the `JavaMapOperator` draws its load profile estimator configuration via the configuration key `wayang.java.map.load`.
Now, it is possible to specify a load profile estimator template in the configuration under the key `<original key>.template`, e.g.:
```xml
wayang.java.map.load.template = {\
  "in":1, "out":1,\
  "cpu":"?*in0"\
}
```
This template specifies a load profile estimator that expects (at least) one input cardinality and one output cardinality.
Further, it models a CPU load that is proportional to the input cardinality.
However, more complex functions are possible.
In particular, you can use
* the variables `in0`, `in1`, ... and `out0`, `out1`, ... to incorporate the input and output cardinalities, respectively;
* operator properties, such as `numIterations` for the `PageRankOperator` implementations;
* the operators `+`, `-`, `*`, `/`, `%`, `^`, and parantheses;
* the functions `min(x0, x1, ...))`, `max(x0, x1, ...)`, `abs(x)`, `log(x, base)`, `ln(x)`, `ld(x)`;
* and the constants `e` and `pi`.

While Wayang specifies templates for all execution operators, you will need to specify that your UDFs are modelled by some configuration-based cost function (see the k-means example below) and create the according initial specification and template yourself.
Once, you gathered execution data, you can run
```shell
java ... org.apache.wayang.profiler.ga.GeneticOptimizerApp [configuration URL [execution log]]
```
This app will try to find appropriate values for the question marks (`?`) in the load profile estimator templates to fit the gathered execution data and ready-made configuration entries for the load profile estimators.
You can then copy them into your configuration.

## Examples

For some executable examples, have a look at [this repository](https://github.com/sekruse/rheem-examples).

### WordCount

The "Hello World!" of data processing systems is the wordcount.

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

Copyright 2020 - 2021 The Apache Software Foundation.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
