<!--

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

-->
This page contains examples to be executed using Wayang.
- [WordCount](#wordcount)
  * [Java scala-like API](#java-scala-like-api)
  * [Scala API](#scala-api)
- [k-means](#k-means)
  * [Scala API](#scala-api-1)

## WordCount

The "Hello World!" of data processing systems is the wordcount.

### Java scala-like API
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

### Scala API

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

## k-means

Wayang is also capable of iterative processing, which is, e.g., very important for machine learning algorithms, such as k-means.

### Scala API

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

## Collaborative Filtering

This code demonstrates the implementation of a Collaborative Filtering algorithm used in Recommendation Systems using Wayang.

```java
import org.apache.wayang.api.*;
import org.apache.wayang.basic.data.*;
import org.apache.wayang.core.api.*;
import org.apache.wayang.core.function.*;
import org.apache.wayang.core.util.*;
import org.apache.wayang.java.Java;
import org.apache.wayang.spark.Spark;
import org.apache.commons.math3.linear.*;

import java.util.*;

public class CollaborativeFiltering {

    public static void main(String[] args) {

        // Create a Wayang context
        WayangContext wayangContext = new WayangContext().with(Java.basicPlugin()).with(Spark.basicPlugin());
        PlanBuilder planBuilder = new PlanBuilder(wayangContext);

        // Load the data
        List<Tuple3<String, String, Integer>> data = Arrays.asList(
            new Tuple3<>("user1", "item1", 5),
            new Tuple3<>("user1", "item2", 3),
            new Tuple3<>("user2", "item1", 4),
            new Tuple3<>("user2", "item3", 2),
            new Tuple3<>("user3", "item2", 1),
            new Tuple3<>("user3", "item3", 5)
        );

        // Define a function to normalize the ratings
        TransformationDescriptor.SerializableFunction<Tuple3<String, String, Integer>, Tuple3<String, String, Double>> normalizationFunction = 
            tuple -> new Tuple3<>(tuple.field0, tuple.field1, (double)tuple.field2 / 5);

        // Define a function to calculate the cosine similarity between users
        TransformationDescriptor.SerializableFunction<Tuple2<String, RealVector>, Tuple2<String, RealVector>> similarityFunction = 
            tuple -> {
                // This is a placeholder. You would need to implement a real similarity calculation here.
                // For example, you could calculate the cosine similarity like this:
                double dotProduct = tuple.field1.dotProduct(otherUserVector);
                double normProduct = tuple.field1.getNorm() * otherUserVector.getNorm();
                double cosineSimilarity = dotProduct / normProduct;
                return new Tuple2<>(tuple.field0, cosineSimilarity);
            };

        // Define a function to calculate the predicted rating for each user-item pair
        TransformationDescriptor.SerializableFunction<Tuple3<String, String, Double>, Tuple3<String, String, Double>> predictionFunction = 
            tuple -> {
                // This is a placeholder. You would need to implement a real prediction calculation here.
                // For example, you could calculate the predicted rating based on the similarity matrix and the user's ratings like this:
                double predictedRating = 0.0;
                double similaritySum = 0.0;
                for (String otherUser : similarityMatrix.keySet()) {
                    double similarity = similarityMatrix.get(otherUser);
                    double otherUserRating = userRatings.get(otherUser).get(tuple.field1);
                    predictedRating += similarity * otherUserRating;
                    similaritySum += Math.abs(similarity);
                }
                predictedRating /= similaritySum;
                return new Tuple3<>(tuple.field0, tuple.field1, predictedRating);
            };

        // Define a function to handle cold start problems
        TransformationDescriptor.SerializableFunction<Tuple3<String, String, Double>, Tuple3<String, String, Double>> coldStartFunction = 
            tuple -> {
                if (tuple.field2 == null) {
                    // If the user has no ratings, recommend the most popular item
                    String mostPopularItem = itemPopularity.entrySet().stream()
                        .max(Map.Entry.comparingByValue())
                        .get()
                        .getKey();
                    return new Tuple3<>(tuple.field0, mostPopularItem, 5.0);
                } else {
                    return tuple;
                }
            };

        // Define a function to handle cold start problems
        TransformationDescriptor.SerializableFunction<Tuple3<String, String, Double>, Tuple3<String, String, Double>> coldStartFunction = 
            tuple -> {
                if (tuple.field2 == null) {
                    // If the user has no ratings, recommend the most popular item
                    return new Tuple3<>(tuple.field0, "item1", 1.0);
                } else {
                    return tuple;
                }
            };

        // Execute the plan
        Collection<Tuple3<String, String, Double>> output = planBuilder
            .loadCollection(data)
            .map(normalizationFunction)
            .map(similarityFunction)
            .map(predictionFunction)
            .map(recommendationFunction)
            .map(coldStartFunction)
            .collect();

        // Print the recommendations
        for (Tuple3<String, String, Double> recommendation : output) {
            System.out.println("User: " + recommendation.field0 + ", Item: " + recommendation.field1 + ", Rating: " + recommendation.field2);
        }
    }
}
```