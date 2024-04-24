# Wayang scala-java API

Wayang provides two options for performing multiple asynchronous tasks - `async` API and `multicontext` API. 

The above examples can be found in the `async.apps` package and the `multicontext.apps` package in wayang-benchmark.

## `async` API
Our example shows how to utilize the `async` API with an example program containing several asynchronous jobs:

```scala
import org.apache.wayang.api.async.DataQuantaImplicits._
import org.apache.wayang.api.async.PlanBuilderImplicits._
import org.apache.wayang.api.{MultiContext, DataQuanta, PlanBuilder}
import org.apache.wayang.java.Java

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Example {

  def main(args: Array[String]): Unit = {
    val planBuilder1 = new PlanBuilder(new MultiContext().withPlugin(Java.basicPlugin())).withUdfJarsOf(this.getClass)
    val planBuilder2 = new PlanBuilder(new MultiContext().withPlugin(Java.basicPlugin())).withUdfJarsOf(this.getClass)
    val planBuilder3 = new PlanBuilder(new MultiContext().withPlugin(Java.basicPlugin())).withUdfJarsOf(this.getClass)

    // Async job 1
    val result1 = planBuilder1
      .loadCollection(List(1, 2, 3, 4, 5))
      .map(_ * 1)
      .runAsync(tempFileOut = "file:///tmp/out1.temp")

    // Async job 2
    val result2 = planBuilder2
      .loadCollection(List(6, 7, 8, 9, 10))
      .filter(_ <= 8)
      .runAsync(tempFileOut = "file:///tmp/out2.temp")

    // Async job 3 which merges 1 and 2
    val dq1: DataQuanta[Int] = planBuilder1.loadAsync(result1)
    val dq2: DataQuanta[Int] = planBuilder1.loadAsync(result2)
    val result3 = dq1.union(dq2)
      .map(_ * 3)
      .filter(_ < 100)
      .runAsync(tempFileOut = "file:///tmp/out3.temp", result1, result2)

    // Async job 4 which runs independently from 1, 2 and 3
    val result4 = planBuilder3
      .loadCollection(List(1, 2, 3, 4, 5))
      .filter(_ >= 2)
      .runAsync(tempFileOut = "file:///tmp/out4.temp")

    // Async job 5 which merges 3 and 4
    val dq3: DataQuanta[Int] = planBuilder1.loadAsync(result3)
    val dq4: DataQuanta[Int] = planBuilder1.loadAsync(result4)
    val result5 = dq3.intersect(dq4)
      .map(_ * 4)
      .writeTextFileAsync(url = "file:///tmp/out5.final", result3, result4)

    Await.result(result5, Duration.Inf)
  }

}
```

**Key Points:**
- Jobs 1, 2, and 4 are executed concurrently as there is no dependency.
 
 
- The output of these jobs is written to the provided path (`.runAsync(tempFileOut = "file:///tmp/out1.temp")`).
 
 
- Job 3 waits for jobs 1 and 2 to finish before starting (`.runAsync(tempFileOut = "file:///tmp/out3.temp", result1, result2)`). It reads the results from the aforementioned paths, unites them, and processes further.
 
 
- Job 5 awaits jobs 3 and 4's completion to begin. This is indicated by  `.writeTextFileAsync("file:///tmp/out5.final", result3, result4)`. Instead of using `runAsync`, we use `writeTextFileAsync` to finish the execution.


## `multicontext` API

The examples below demonstrate the capabilities of the multi context api.

### Basic usage

```scala
import org.apache.wayang.api.{MultiContext, MultiContextPlanBuilder}
import org.apache.wayang.core.api.Configuration
import org.apache.wayang.java.Java
import org.apache.wayang.spark.Spark

class WordCount {}

object WordCount {

  def main(args: Array[String]): Unit = {
    println("WordCount")
    println("Scala version:")
    println(scala.util.Properties.versionString)

    val configuration1 = new Configuration()
    val configuration2 = new Configuration()

    val context1 = new MultiContext(configuration1)
      .withPlugin(Java.basicPlugin())
      .withTextFileSink("file:///tmp/out11")
    val context2 = new MultiContext(configuration2)
      .withPlugin(Spark.basicPlugin())
      .withTextFileSink("file:///tmp/out12")

    val multiContextPlanBuilder = new MultiContextPlanBuilder(List(context1, context2))
      .withUdfJarsOf(this.getClass)

    // Generate some test data
    val inputValues = Array("Big data is big.", "Is data big data?")

    // Build and execute a word count
    multiContextPlanBuilder.forEach(_
      .loadCollection(inputValues)
      .flatMap(_.split("\\s+"))
      .map(_.replaceAll("\\W+", "").toLowerCase)
      .map((_, 1))
      .reduceByKey(_._1, (a, b) => (a._1, a._2 + b._2))
    ).execute()

  }
}
```

The above program executes the same plan concurrently for two contexts, `context1` which runs on Java and writes to text file `file:///tmp/out11`, and `context2` which runs on Spark (and can be configured through `configuration2`) and writes to `file:///tmp/out12`.

The same job can also be written like this:

```scala
multiContextPlanBuilder
    .forEach(_.loadCollection(inputValues))
    .forEach(_.flatMap(_.split("\\s+")))
    .forEach(_.map(_.replaceAll("\\W+", "").toLowerCase))
    .forEach(_.map((_, 1)))
    .forEach(_.reduceByKey(_._1, (a, b) => (a._1, a._2 + b._2)))
    .execute()
```

### Target platforms

```scala
import org.apache.wayang.api.{MultiContext, MultiContextPlanBuilder}
import org.apache.wayang.java.Java
import org.apache.wayang.spark.Spark

object WordCountWithTargetPlatforms {

  def main(args: Array[String]): Unit = {
    val configuration1 = new Configuration()
    val configuration2 = new Configuration()

    val context1 = new MultiContext(configuration1)
      .withPlugin(Java.basicPlugin())
      .withPlugin(Spark.basicPlugin())
      .withTextFileSink("file:///tmp/out11")
    val context2 = new MultiContext(configuration2)
      .withPlugin(Java.basicPlugin())
      .withPlugin(Spark.basicPlugin())
      .withTextFileSink("file:///tmp/out12")

    val multiContextPlanBuilder = new MultiContextPlanBuilder(List(context1, context2))
      .withUdfJarsOf(this.getClass)

    // Generate some test data
    val inputValues1 = Array("Big data is big.", "Is data big data?")
    val inputValues2 = Array("Big big data is big big.", "Is data big data big?")

    multiContextPlanBuilder
      .loadCollection(context1, inputValues1)
      .loadCollection(context2, inputValues2)

      .forEach(_.flatMap(_.split("\\s+")))
      .withTargetPlatforms(context1, Spark.platform())
      .withTargetPlatforms(context2, Java.platform())

      .forEach(_.map(_.replaceAll("\\W+", "").toLowerCase))
      .withTargetPlatforms(Java.platform())

      .forEach(_.map((_, 1)))

      .forEach(_.reduceByKey(_._1, (a, b) => (a._1, a._2 + b._2)))
      .withTargetPlatforms(context1, Spark.platform())
      .execute()
  }
}
```

Here we add the ability to execute an operation on different platforms for each context. 

In the snippet below

```scala
      .forEach(_.flatMap(_.split("\\s+")))
      .withTargetPlatforms(context1, Spark.platform())
      .withTargetPlatforms(context2, Java.platform())
```

the `flatMap` operator will be executed on Spark for `context1` and on Java for `context2`.

In the snippet below

```scala
      .forEach(_.map(_.replaceAll("\\W+", "").toLowerCase))
      .withTargetPlatforms(Java.platform())
```

the `map` operator gets executed on the Java platform for all contexts, since none is specifically stated.

In the snippet below

```scala
      .forEach(_.reduceByKey(_._1, (a, b) => (a._1, a._2 + b._2)))
      .withTargetPlatforms(context1, Spark.platform())
```

the `reduceByKey` operator will be executed on spark for `context1` and on the default platform (decided by the optimzer) for `context2.

### Merge

```scala
import org.apache.wayang.api.{MultiContext, MultiContextPlanBuilder}
import org.apache.wayang.core.api.{Configuration, WayangContext}
import org.apache.wayang.java.Java
import org.apache.wayang.spark.Spark

object WordCountWithMerge {

  def main(args: Array[String]): Unit = {
    val configuration1 = new Configuration()
    val configuration2 = new Configuration()

    val context1 = new MultiContext(configuration1)
      .withPlugin(Java.basicPlugin())
      .withMergeFileSink("file:///tmp/out11") // The mergeContext will read the output of context 1 from here
    val context2 = new MultiContext(configuration2)
      .withPlugin(Java.basicPlugin())
      .withMergeFileSink("file:///tmp/out12") // The mergeContext will read the output of context 2 from here

    val multiContextPlanBuilder = new MultiContextPlanBuilder(List(context1, context2))
      .withUdfJarsOf(this.getClass)

    // To be used after merging the previous two
    val mergeContext = new WayangContext(new Configuration())
      .withPlugin(Java.basicPlugin())

    // Generate some test data
    val inputValues1 = Array("Big data is big.", "Is data big data?")
    val inputValues2 = Array("Big big data is big big.", "Is data big data big?")

    // Build and execute a word count in 2 different contexts
    multiContextPlanBuilder
      .loadCollection(context1, inputValues1)
      .loadCollection(context2, inputValues2)
      .forEach(_.flatMap(_.split("\\s+")))
      .forEach(_.map(_.replaceAll("\\W+", "").toLowerCase))
      .forEach(_.map((_, 1)))
      .forEach(_.reduceByKey(_._1, (a, b) => (a._1, a._2 + b._2)))

      // Merge contexts with union operator
      .mergeUnion(mergeContext)

      // Continue processing merged DataQuanta
      .filter(_._2 >= 3)
      .reduceByKey(_._1, (t1, t2) => (t1._1, t1._2 + t2._2))

      // Write out
      // Writes:
      //    (big,9)
      //    (data,6)
      .writeTextFile("file:///tmp/out1.merged", s => s.toString())

  }

}
```

Here the two contexts start executing concurrently as in the above examples, but they get merged here

```scala
      .mergeUnion(mergeContext)
```

The merging is happening at the merge context which can be one of the above or a completely new one. 

Note that the merge context reads the data to merge from the paths specified for each context here

```scala
      .withMergeFileSink("file:///tmp/out11")   // The mergeContext will read the output of context 1 from here
      ...
      .withMergeFileSink("file:///tmp/out12")   // The mergeContext will read the output of context 2 from here
      ...
```

The rest of the execution happens at the merge context and is a now single job.

### Combine each

```scala
import org.apache.wayang.api.{MultiContext, DataQuanta, MultiContextPlanBuilder}
import org.apache.wayang.java.Java

object WordCountCombineEach {

  def main(args: Array[String]): Unit = {
    val configuration1 = new Configuration()
    val configuration2 = new Configuration()

    val context1 = new MultiContext(configuration1)
      .withPlugin(Java.basicPlugin())
      .withTextFileSink("file:///tmp/out11")
    val context2 = new MultiContext(configuration2)
      .withPlugin(Java.basicPlugin())
      .withTextFileSink("file:///tmp/out12")

    val multiContextPlanBuilder = new MultiContextPlanBuilder(List(context1, context2))
      .withUdfJarsOf(this.getClass)

    // Generate some test data
    val inputValues = Array("Big data is big.", "Is data big data?")

    // Build and execute a word count
    val dq1 = multiContextPlanBuilder
      .forEach(_.loadCollection(inputValues))
      .forEach(_.flatMap(_.split("\\s+")))
      .forEach(_.map(_.replaceAll("\\W+", "").toLowerCase))
      .forEach(_.map((_, 1)))
      .forEach(_.reduceByKey(_._1, (a, b) => (a._1, a._2 + b._2)))

    val dq2 = multiContextPlanBuilder
      .forEach(_.loadCollection(inputValues))
      .forEach(_.flatMap(_.split("\\s+")))
      .forEach(_.map(_.replaceAll("\\W+", "").toLowerCase))
      .forEach(_.map((_, 1)))
      .forEach(_.reduceByKey(_._1, (a, _) => (a._1, 100)))

    dq1.combineEach(dq2, (dq1: DataQuanta[(String, Int)], dq2: DataQuanta[(String, Int)]) => dq1.union(dq2))
      .forEach(_.map(t => (t._1 + " wayang out", t._2)))
      .execute()
  }

}
```

Here we add the capability of performing a binary operator on each context. 

With this line

```scala
    dq1.combineEach(dq2, (dq1: DataQuanta[(String, Int)], dq2: DataQuanta[(String, Int)]) => dq1.union(dq2))
```

the `dq1.union(dq2)` operation is happening for each context. 

The rest of the execution

```scala
      .forEach(_.map(t => (t._1 + " wayang out", t._2)))
      .execute()
```

continues as a multi context job. 