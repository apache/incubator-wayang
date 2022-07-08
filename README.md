# Apache Wayang (incubating) <img align="right" width="128px" src="https://wayang.apache.org/assets/img/logo/logo_400x160.png" alt="Wayang logo">
#### The first cross-platform data processing system

![Travis branch](https://img.shields.io/travis/com/apache/incubator-wayang/main?style=for-the-badge)
[![Maven central](https://img.shields.io/maven-central/v/org.apache.wayang/wayang-core.svg?style=for-the-badge)](https://img.shields.io/maven-central/v/org.apache.wayang/wayang-core.svg)
[![License](https://img.shields.io/github/license/apache/incubator-wayang.svg?style=for-the-badge)](http://www.apache.org/licenses/LICENSE-2.0)
[![Last commit](https://img.shields.io/github/last-commit/apache/incubator-wayang.svg?style=for-the-badge)]()
![GitHub commit activity (branch)](https://img.shields.io/github/commit-activity/m/apache/incubator-wayang?style=for-the-badge)
![GitHub forks](https://img.shields.io/github/forks/apache/incubator-wayang?style=for-the-badge)
![GitHub Repo stars](https://img.shields.io/github/stars/apache/incubator-wayang?style=for-the-badge)

[![Tweet](https://img.shields.io/twitter/url/http/shields.io.svg?style=social)](https://twitter.com/intent/tweet?text=Apache%20Wayang%20enables%20cross%20platform%20data%20processing,%20star%20it%20via:%20&url=https://github.com/apache/incubator-wayang&via=apachewayang&hashtags=dataprocessing,bigdata,analytics,hybridcloud,developers) [![Subreddit subscribers](https://img.shields.io/reddit/subreddit-subscribers/ApacheWayang?style=social)](https://www.reddit.com/r/ApacheWayang/)


## Description

In contrast to traditional data processing systems that provide one dedicated execution engine, Apache Wayang (incubating) can transparently use mutliple execution engines to perform a single task. We call this *cross-platform data processing*. In Wayang, users can specify any data processing application using one of Wayang's APIs and then Wayang will choose the data processing platform(s), e.g., Postgres or Apache Spark, that best fits the application. Finally, Wayang will perform the execution, thereby hiding the different platform-specific APIs and coordinating inter-platform communication.

Apache Wayang (incubating) aims at freeing data engineers and software developers from the burden of learning all different data processing systems, their APIs, strengths and weaknesses; the intricacies of coordinating and integrating different processing platforms; and the inflexibility when trying a fixed set of processing platforms. As of now, Wayang has built-in support for the following processing platforms:
- [Java Streams](https://docs.oracle.com/javase/8/docs/api/java/util/stream/Stream.html)
- [Apache Spark](https://spark.apache.org/)
- [Apache Flink](https://flink.apache.org/)
- [Apache Giraph](https://giraph.apache.org/)
- [GraphChi](https://github.com/GraphChi/graphchi-java)
- [Postgres](http://www.postgresql.org)
- [SQLite](https://www.sqlite.org/)

## Installing Wayang

You can download wayang from [here](https://github.com/apache/incubator-wayang/releases/download/wayang-0.6.1-alpha-rc/wayang-0.6.1-alpha-rc.tar.gz), and to install you need to follow the next steps:

```shell
tar -xvf wayang-0.6.1-snapshot.tar.gz
cd wayang-0.6.1-SNAPSHOT
```

In linux
```shell 
echo "export WAYANG_HOME=$(pwd)" >> ~/.bashrc
echo "export PATH=${PATH}:${WAYANG_HOME}/bin" >> ~/.bashrc
source ~/.bashrc
```
In MacOS
```shell 
echo "export WAYANG_HOME=$(pwd)" >> ~/.zshrc
echo "export PATH=${PATH}:${WAYANG_HOME}/bin" >> ~/.zshrc
source ~/.zshrc
```

### Requirements at Runtime

Apache Wayang(incubating) is not at execution engine, but it administrate the execution engines for 
you. Because of it is important to have installed the following requirements

- Java version support from 8, the wayang team recommend Java version 11, does not forget to declare
   the variable `JAVA_HOME`
- Apache Spark, you need to installed Apache Spark from version 3, does not forget to have declare
   the variable `SPARK_HOME`
- Apache Hadoop, you need to installed Apache Hadoop from version 3, does not forget to have declare
  the variable `HADOOP_HOME`

### Validating the installation

To execute your first code in wayang you need to execute the following command

```shell
wayang-submit org.apache.wayang.apps.wordcount.Main java file://$(pwd)/README.md
```

## Getting Started

Wayang is available via Maven Central. To use it with Maven, include the following into your POM file:
```xml
<dependency> 
  <groupId>org.apache.wayang</groupId>
  <artifactId>wayang-***</artifactId>
  <version>0.6.0</version> 
</dependency>
```
Note the `***`: Wayang ships with multiple modules that can be included in your app, depending on how you want to use it:
* `wayang-core`: provides core data structures and the optimizer (required)
* `wayang-basic`: provides common operators and data types for your apps (recommended)
* `wayang-api-scala-java_2.12`: provides an easy-to-use Scala and Java API to assemble Wayang plans (recommended)
* `wayang-java`, `wayang-spark`, `wayang-graphchi`, `wayang-sqlite3`, `wayang-postgres`: adapters for the various supported processing platforms
* `wayang-profiler`: provides functionality to learn operator and UDF cost functions from historical execution data

> **NOTE:** The module `wayang-api-scala-java_2.12` is intended to be used with Java 11 and Scala 2.12. If you have the Java 8 version, you need to use the `wayang-api-scala-java_2.11` module.


For the sake of version flexibility, you still have to include in the POM file your Hadoop (`hadoop-hdfs` and `hadoop-common`) and Spark (`spark-core` and `spark-graphx`) version of choice.

In addition, you can obtain the most recent snapshot version of Wayang via Sonatype's snapshot repository. Just include:
```xml
<repositories>
  <repository>
    <id>apache-snapshots</id>
    <name>Apache Foundation Snapshot Repository</name>
    <url>https://repository.apache.org/content/repositories/snapshots</url>
  </repository>
</repositories>
```

### Prerequisites
Apache Wayang (incubating) is built with Java 1 and Scala 2.12. However, to run Wayang it is sufficient to have just Java 11 installed. Please also consider that processing platforms employed by Wayang might have further requirements.
```
Java 11
[Scala 2.12]
```

> **NOTE:** Wayang also works with Java 8 and Scala 2.11. If you want to use these versions, you will have to re-build Wayang (see below).

> **NOTE:** In windows, you need to define the variable `HADOOP_HOME` with the winutils.exe, an not official option to obtain [this repository](https://github.com/steveloughran/winutils), or you can generate your winutils.exe following the instructions in the repository. Also, you may need to install [msvcr100.dll](https://www.microsoft.com/en-us/download/details.aspx?id=26999)

> **NOTE:** Make sure that the JAVA_HOME environment variable is set correctly to either Java 8 or Java 11 as the prerequisite checker script currently supports up to Java 11 and checks the latest version of Java if you have higher version installed. In Linux, it is preferably to use the export JAVA_HOME method inside the project folder. It is also recommended running './mvnw clean install' before opening the project using IntelliJ.


### Building

If you need to rebuild Wayang, e.g., to use a different Scala version, you can simply do so via Maven:

1. Adapt the version variables (e.g., `spark.version`) in the main `pom.xml` file.
2. Build Wayang with the adapted versions.
    ```shell
   git clone https://github.com/apache/incubator-wayang.git
   cd incubator-wayang
   ./mvnw clean install -DskipTests
    ```
> **NOTE:** If you receive an error about not finding `MathExBaseVisitor`, then the problem might be that you are trying to build from IntelliJ, without Maven. MathExBaseVisitor is generated code, and a Maven build should generate it automatically.

> **NOTE:** In the current Maven setup, the version of scala is tied to the Java version, you can compile the profile `scala-11` with Java 8 and profile `scala-12` with Java 11.

> **NOTE:** For compiling and testing the code it is required to have Hadoop installed on your machine.

> **NOTE:**  the `standalone` profile to fix Hadoop and Spark versions, so that Wayang apps do not explicitly need to declare the corresponding dependencies.
>
> Also, note the `distro` profile, which assembles a binary Wayang distribution.
To activate these profiles, you need to specify them when running maven, i.e.,

```shell
./mvnw clean install -DskipTests -P<profile name> 
```

## Running the tests
In the incubator-wayang root folder run:
```shell
./mvnw test
```

## Example Applications
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

## Built With

* [Java 11](https://www.oracle.com/de/java/technologies/javase/jdk11-archive-downloads.html) 
* [Scala 2.12](https://www.scala-lang.org/download/2.12.0.html)
* [Maven](https://maven.apache.org/)

## Contributing
[Contact](dev@wayang.apache.org) us if you are looking for tasks to contribute.

When contributing code please adhere with the [Apache code of conduct](https://www.apache.org/foundation/policies/conduct.html).

## Authors

See the list of [contributors](https://github.com/apache/incubator-wayang/graphs/contributors) who participated in this project.

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

## Acknowledgements
The [Logo](http://wayang.apache.org/assets/img/logo/Apache_Wayang/Apache_Wayang.pdf) was donated by [Brian Vera](https://www.linkedin.com/in/brian-vera-hablares-17a663b8/).
