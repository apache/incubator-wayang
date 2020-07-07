# Rheem <img align="right" width="128px" src="http://da.qcri.org/rheem/img/logo.png" alt="Rheem logo">

[![Build Status (Jenkins)](https://jenkins.qcri.org/buildStatus/icon?job=rheem-build)](https://jenkins.qcri.org/job/rheem-build/)
[![Build Status (Travis)](https://travis-ci.org/rheem-ecosystem/rheem.svg?branch=master)](https://travis-ci.org/rheem-ecosystem/rheem)
[![Gitter chat](https://badges.gitter.im/rheem-ecosystem/Lobby.png)](https://gitter.im/rheem-ecosystem/Lobby)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.qcri.rheem/rheem/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.qcri.rheem/rheem)

#### Turning the Zoo of Data Processing Systems into a Circus


Rheem is an efficient and scalable data processing framework developed by the [data analytics](http://da.qcri.org) group at [Qatar Computing Research Institute](http://qcri.com/) in collaboration with the [information systems group](https://www.hpi.de/naumann) at the Hasso Plattner Institute. In contrast to classical data processing systems that provide one dedicated execution engine, Rheem rather is a *meta processing framework*: You can specify your data processing app via one of Rheem's API and then Rheem will pick an optimal configuration of classical processing frameworks, such as Java Streams or Apache Spark, to run your app on. Finally, Rheem will also perform the execution, thereby hiding the different specific platform APIs and coordinate inter-platform communication.

This approach aims at freeing data engineers and software developers from the burden of knowing the zoo of different data processing systems, their APIs, strengths and weakness; the intricacies of coordinating and integrating different processing platforms; and the inflexibility when tying to a fix set of processing platforms. As of now, Rheem has built in support for the following processing platforms:
- Java 8 Streams
- [Apache Spark](https://spark.apache.org/)
- [GraphChi](https://github.com/GraphChi/graphchi-java)
- [Postgres](http://www.postgresql.org)
- [SQLite](https://www.sqlite.org/)

## How to use Rheem

**Requirements.**
Rheem is built with Java 8 and Scala 2.11. However, to execute Rheem it is sufficient to have Java 8 installed. If you want to build Rheem yourself, you will also need to have [Apache Maven](http://maven.apache.org) installed. Please also consider that processing platforms employed by Rheem might have further requirements.

**Get Rheem.**
Rheem is available via Maven Central. To use it with Maven, for instance, include the following into you POM file:
```xml
<dependency> 
  <groupId>org.qcri.rheem</groupId>
  <artifactId>rheem-***</artifactId>
  <version>0.3.0</version> 
</dependency>
```
Note the `***`: Rheem ships with multiple modules that can be included in your app, depending on how you want to use it:
* `rheem-core`: provides core data structures and the optimizer (required)
* `rheem-basic`: provides common operators and data types for your apps (recommended)
* `rheem-api`: provides an easy-to-use Scala and Java API to assemble Rheem plans (recommended)
* `rheem-java`, `rheem-spark`, `rheem-graphchi`, `rheem-sqlite3`, `rheem-postgres`: adapters for the various supported processing platforms
* `rheem-profiler`: provides functionality to learn operator and UDF cost functions from historical execution data

For the sake of version flexibility, you still have to include your Hadoop (`hadoop-hdfs` and `hadoop-common`) and Spark (`spark-core` and `spark-graphx`) version of choice.

In addition, you can obtain the most recent snapshot version of Rheem via Sonatype's snapshot repository. Just included
```xml
<repositories>
  <repository>
    <id>sonatype-snapshots</id>
    <name>Sonatype Snapshot Repository</name>
    <url>https://oss.sonatype.org/content/repositories/snapshots</url>
  </repository>
<repositories>
```

If you need to rebuild Rheem, e.g., to use a different Scala version, you can simply do so via Maven:

1. Adapt the version variables (e.g., `spark.version`) in the main `pom.xml` file.
2. Build Rheem with the adapted versions.
	```shell
	$ mvn clean install
	```
	Note the `standalone` profile to fix Hadoop and Spark versions, so that Rheem apps do not explicitly need to declare the corresponding dependencies.
	Also, note the `distro` profile, which assembles a binary Rheem distribution.
	To activate these profiles, you need to specify them when running maven, i.e.,
	 ```shell
	 mvn clean install -P<profile name>
	 ```

**Configure Rheem.** In order for Rheem to work properly, it is necessary to tell Rheem about the capacities of your processing platforms and how to reach them. While there is a default configuration that allows to test Rheem right away, we recommend to create a properties file to adapt the configuration where necessary. To have Rheem use that configuration transparently, just run you app via
```shell
$ java -Drheem.configuration=url://to/my/rheem.properties ...
```

You can find the most relevant settings in the following:
* General settings
  * `rheem.core.log.enabled (= true)`: whether to log execution statistics to allow learning better cardinality and cost estimators for the optimizer
  * `rheem.core.log.executions (= ~/.rheem/executions.json)` where to log execution times of operator groups
  * `rheem.core.log.cardinalities (= ~/.rheem/cardinalities.json)` where to log cardinality measurements
  * `rheem.core.optimizer.instrumentation (= org.qcri.rheem.core.profiling.OutboundInstrumentationStrategy)`: where to measure cardinalities in Rheem plans; other options are `org.qcri.rheem.core.profiling.NoInstrumentationStrategy` and `org.qcri.rheem.core.profiling.FullInstrumentationStrategy`
  * `rheem.core.optimizer.reoptimize (= false)`: whether to progressively optimize Rheem plans
  * `rheem.basic.tempdir (= file:///tmp)`: where to store temporary files, in particular for inter-platform communication
* Java Streams
  * `rheem.java.cpu.mhz (= 2700)`: clock frequency of processor the JVM runs on in MHz
  * `rheem.java.hdfs.ms-per-mb (= 2.7)`: average throughput from HDFS to JVM in ms/MB
* Apache Spark
  * `spark.master (= local)`: Spark master
    * various other Spark settings are supported, e.g., `spark.executor.memory`, `spark.serializer`, ...
  * `rheem.spark.cpu.mhz (= 2700)`: clock frequency of processor the Spark workers run on in MHz
  * `rheem.spark.hdfs.ms-per-mb (= 2.7)`: average throughput from HDFS to the Spark workers in ms/MB
  * `rheem.spark.network.ms-per-mb (= 8.6)`: average network throughput of the Spark workers in ms/MB
  * `rheem.spark.init.ms (= 4500)`: time it takes Spark to initialize in ms
* GraphChi
  * `rheem.graphchi.cpu.mhz (= 2700)`: clock frequency of processor GraphChi runs on in MHz
  * `rheem.graphchi.cpu.cores (= 2)`: number of cores GraphChi runs on
  * `rheem.graphchi.hdfs.ms-per-mb (= 2.7)`: average throughput from HDFS to GraphChi in ms/MB
* SQLite
  * `rheem.sqlite3.jdbc.url`: JDBC URL to use SQLite
  * `rheem.sqlite3.jdbc.user`: optional user name
  * `rheem.sqlite3.jdbc.password`: optional password
  * `rheem.sqlite3.cpu.mhz (= 2700)`: clock frequency of processor SQLite runs on in MHz
  * `rheem.sqlite3.cpu.cores (= 2)`: number of cores SQLite runs on
* PostgreSQL
  * `rheem.postgres.jdbc.url`: JDBC URL to use PostgreSQL
  * `rheem.postgres.jdbc.user`: optional user name
  * `rheem.postgres.jdbc.password`: optional password
  * `rheem.postgres.cpu.mhz (= 2700)`: clock frequency of processor PostgreSQL runs on in MHz
  * `rheem.postgres.cpu.cores (= 2)`: number of cores PostgreSQL runs on

**Code with Rheem.** The recommended way to specify your apps with Rheem is via its Scala or Java API from the `rheem-api` module. You can find examples below.

**Learn cost functions.**
Rheem provides a utility to learn cost functions from historical execution data.
Specifically, Rheem can learn configurations for load profile estimators (that estimate CPU load, disk load etc.) for both operators and UDFs, as long as the configuration provides a template for those estimators.
As an example, the `JavaMapOperator` draws its load profile estimator configuration via the configuration key `rheem.java.map.load`.
Now, it is possible to specify a load profile estimator template in the configuration under the key `<original key>.template`, e.g.:
```xml
rheem.java.map.load.template = {\
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

While Rheem specifies templates for all execution operators, you will need to specify that your UDFs are modelled by some configuration-based cost function (see the k-means example below) and create the according initial specification and template yourself.
Once, you gathered execution data, you can run
```shell
java ... org.qcri.rheem.profiler.ga.GeneticOptimizerApp [configuration URL [execution log]]
```
This app will try to find appropriate values for the question marks (`?`) in the load profile estimator templates to fit the gathered execution data and ready-made configuration entries for the load profile estimators.
You can then copy them into your configuration.

## Examples

For some executable examples, have a look at [this repository](https://www.github.com/sekruse/rheem-examples).

### WordCount

The "Hello World!" of data processing systems is the wordcount.

#### Java API
```java
import org.qcri.rheem.api.JavaPlanBuilder;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.qcri.rheem.java.Java;
import org.qcri.rheem.spark.Spark;
import java.util.Collection;
import java.util.Arrays;

public class WordcountJava {

    public static void main(String[] args){

        // Settings
        String inputUrl = "file:/tmp.txt";

        // Get a plan builder.
        RheemContext rheemContext = new RheemContext(new Configuration())
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin());
        JavaPlanBuilder planBuilder = new JavaPlanBuilder(rheemContext)
                .withJobName(String.format("WordCount (%s)", inputUrl))
                .withUdfJarOf(WordcountJava.class);

        // Start building the RheemPlan.
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
import org.qcri.rheem.api._
import org.qcri.rheem.core.api.{Configuration, RheemContext}
import org.qcri.rheem.java.Java
import org.qcri.rheem.spark.Spark

object WordcountScala {
  def main(args: Array[String]) {

    // Settings
    val inputUrl = "file:/tmp.txt"

    // Get a plan builder.
    val rheemContext = new RheemContext(new Configuration)
      .withPlugin(Java.basicPlugin)
      .withPlugin(Spark.basicPlugin)
    val planBuilder = new PlanBuilder(rheemContext)
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

Rheem is also capable of iterative processing, which is, e.g., very important for machine learning algorithms, such as k-means.

#### Scala API

```scala
import org.qcri.rheem.api._
import org.qcri.rheem.core.api.{Configuration, RheemContext}
import org.qcri.rheem.core.function.FunctionDescriptor.ExtendedSerializableFunction
import org.qcri.rheem.core.function.ExecutionContext
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimators
import org.qcri.rheem.java.Java
import org.qcri.rheem.spark.Spark

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
    val rheemContext = new RheemContext(new Configuration)
      .withPlugin(Java.basicPlugin)
      .withPlugin(Spark.basicPlugin)
    val planBuilder = new PlanBuilder(rheemContext)
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

Unless explicitly stated otherwise all files in this repository are licensed under the Apache Software License 2.0

Copyright 2016 Qatar Computing Research Institute

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
