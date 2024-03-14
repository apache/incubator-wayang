/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.api.json.builder

import org.apache.wayang.api.json.parserutil.ParseScalaFromString
import org.apache.wayang.api.json.operatorfromjson.{ComposedOperatorFromJson, OperatorFromJson}
import org.apache.wayang.api.json.operatorfromjson.binary.{CartesianOperatorFromJson, CoGroupOperatorFromJson, IntersectOperatorFromJson, JoinOperatorFromJson, UnionOperatorFromJson}
import org.apache.wayang.api.json.operatorfromjson.other.KMeansFromJson
import org.apache.wayang.api.json.operatorfromjson.input.{InputCollectionFromJson, JDBCRemoteInputFromJson, TableInputFromJson, TextFileInputFromJson}
import org.apache.wayang.api.json.operatorfromjson.loop.{DoWhileOperatorFromJson, ForeachOperatorFromJson, RepeatOperatorFromJson}
import org.apache.wayang.api.json.operatorfromjson.output.TextFileOutputFromJson
import org.apache.wayang.api.json.operatorfromjson.unary.{CountOperatorFromJson, DistinctOperatorFromJson, FilterOperatorFromJson, FlatMapOperatorFromJson, GroupByOpeartorFromJson, MapOperatorFromJson, MapPartitionsOperatorFromJson, ReduceByOperatorFromJson, ReduceOperatorFromJson, SampleOperatorFromJson, SortOperatorFromJson}
import org.apache.wayang.api._
import org.apache.wayang.basic.operators.TableSource
import org.apache.wayang.core.api.{Configuration, WayangContext}
import org.apache.wayang.core.function.ExecutionContext
import org.apache.wayang.core.function.FunctionDescriptor.ExtendedSerializableFunction
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.java.Java
import org.apache.wayang.postgres.Postgres
import org.apache.wayang.postgres.operators.PostgresTableSource
import org.apache.wayang.spark.Spark

import scala.collection.JavaConverters._

class JsonPlanBuilder {

  var planBuilder: PlanBuilder = _
  var operators: Map[Long, OperatorFromJson] = Map()

  def setOperators(operators: List[OperatorFromJson]): JsonPlanBuilder = {
    setOperatorsRec(operators)

    val configuration = new Configuration

    val wayangContext = new WayangContext(configuration)
      //.withPlugin(Java.basicPlugin)
      .withPlugin(Spark.basicPlugin)

    /*
      TODO:
      if (java_set)
        wayang_context.withPlugin(java_plugin)
      if (spark_set)
        wayang_context.withPlugin(spark_plugin)
      if (hadoop_set)
        wayang_context.withPlugin(hadoop_plugin)
     */

    // Check if there is a jdbc remote input. If yes, set configuration appropriately
    operators.foreach {
      case jdbcRemoteInputFromJson: JDBCRemoteInputFromJson =>
        configuration.setProperty("wayang.postgres.jdbc.url", jdbcRemoteInputFromJson.data.uri)
        configuration.setProperty("wayang.postgres.jdbc.user", jdbcRemoteInputFromJson.data.username)
        configuration.setProperty("wayang.postgres.jdbc.password", jdbcRemoteInputFromJson.data.password)
        wayangContext.withPlugin(Postgres.plugin())
      case _ =>
    }

    planBuilder = new PlanBuilder(wayangContext).withUdfJarsOf(this.getClass)

    this
  }

  private def setOperatorsRec(operators: List[OperatorFromJson]): JsonPlanBuilder = {
    for (operator <- operators) {
      operator match {
        case composedOperatorFromJson: ComposedOperatorFromJson => setOperatorsRec(composedOperatorFromJson.operators.toList)
        case operator => setOperatorsMapAndConnectionsMap(operator)
      }
    }
    this
  }

  private def setOperatorsMapAndConnectionsMap(operator: OperatorFromJson): Unit = {
    this.operators = this.operators + (operator.id -> operator)
  }

  def execute(): DataQuanta[Any] = {
    var outputOperators: List[OperatorFromJson] = operators.values.filter(operator => operator.cat == OperatorFromJson.Categories.Output).toList
    if (outputOperators.isEmpty)
      outputOperators = operators.values.filter(operator => operator.output.length == 0).toList
    val outputOperator = outputOperators.head
    executeRecursive(outputOperator, planBuilder)
  }

  private def executeRecursive(operator: OperatorFromJson, planBuilder: PlanBuilder): DataQuanta[Any] = {
    operator match {
      // input
      case inputOperator: TextFileInputFromJson => visit(inputOperator, planBuilder)
      case inputOperator: InputCollectionFromJson => visit(inputOperator, planBuilder)
      case inputOperator: TableInputFromJson => visit(inputOperator, planBuilder)
      case inputOperator: JDBCRemoteInputFromJson => visit(inputOperator, planBuilder)

      // output
      case outputOperator: TextFileOutputFromJson => visit(outputOperator, executeRecursive(this.operators(operator.input(0)), planBuilder))

      // unary
      case operator: MapOperatorFromJson => this.visit(operator, executeRecursive(this.operators(operator.input(0)), planBuilder))
      case operator: FilterOperatorFromJson => this.visit(operator, executeRecursive(this.operators(operator.input(0)), planBuilder))
      case operator: FlatMapOperatorFromJson => this.visit(operator, executeRecursive(this.operators(operator.input(0)), planBuilder))
      case operator: ReduceByOperatorFromJson => this.visit(operator, executeRecursive(this.operators(operator.input(0)), planBuilder))
      case operator: CountOperatorFromJson => this.visit(operator, executeRecursive(this.operators(operator.input(0)), planBuilder))
      case operator: GroupByOpeartorFromJson => this.visit(operator, executeRecursive(this.operators(operator.input(0)), planBuilder))
      case operator: SortOperatorFromJson => this.visit(operator, executeRecursive(this.operators(operator.input(0)), planBuilder))
      case operator: DistinctOperatorFromJson => this.visit(operator, executeRecursive(this.operators(operator.input(0)), planBuilder))
      case operator: ReduceOperatorFromJson => this.visit(operator, executeRecursive(this.operators(operator.input(0)), planBuilder))
      case operator: SampleOperatorFromJson => this.visit(operator, executeRecursive(this.operators(operator.input(0)), planBuilder))
      case operator: MapPartitionsOperatorFromJson => this.visit(operator, executeRecursive(this.operators(operator.input(0)), planBuilder))

      // binary
      case operator: UnionOperatorFromJson => this.visit(operator, executeRecursive(this.operators(operator.input(0)), planBuilder), executeRecursive(this.operators(operator.input(1)), planBuilder))
      case operator: JoinOperatorFromJson => this.visit(operator, executeRecursive(this.operators(operator.input(0)), planBuilder), executeRecursive(this.operators(operator.input(1)), planBuilder))
      case operator: CartesianOperatorFromJson => this.visit(operator, executeRecursive(this.operators(operator.input(0)), planBuilder), executeRecursive(this.operators(operator.input(1)), planBuilder))
      case operator: CoGroupOperatorFromJson => this.visit(operator, executeRecursive(this.operators(operator.input(0)), planBuilder), executeRecursive(this.operators(operator.input(1)), planBuilder))
      case operator: IntersectOperatorFromJson => this.visit(operator, executeRecursive(this.operators(operator.input(0)), planBuilder), executeRecursive(this.operators(operator.input(1)), planBuilder))

      // loop
      case operator: DoWhileOperatorFromJson => this.visit(operator, executeRecursive(this.operators(operator.input(0)), planBuilder))
      case operator: RepeatOperatorFromJson => this.visit(operator, executeRecursive(this.operators(operator.input(0)), planBuilder))
      case operator: ForeachOperatorFromJson => this.visit(operator, executeRecursive(this.operators(operator.input(0)), planBuilder))

      // Other
      case operator: KMeansFromJson => this.visit(operator, executeRecursive(this.operators(operator.input(0)), planBuilder))
    }
  }

  //
  // Input operators
  //

  private def visit(operator: TextFileInputFromJson, planBuilder: PlanBuilder): DataQuanta[Any] = {
    planBuilder.readTextFile(operator.data.filename).asInstanceOf[DataQuanta[Any]]
  }

  private def visit(operator: InputCollectionFromJson, planBuilder: PlanBuilder): DataQuanta[Any] = {
    planBuilder.loadCollection(ParseScalaFromString.parseIterable(operator.data.udf))
  }

  private def visit(operator: TableInputFromJson, planBuilder: PlanBuilder): DataQuanta[Any] = {
    planBuilder.readTable(new TableSource(operator.data.tableName, operator.data.columnNames: _*)).asInstanceOf[DataQuanta[Any]]
  }

  private def visit(operator: JDBCRemoteInputFromJson, planBuilder: PlanBuilder): DataQuanta[Any] = {
    planBuilder.readTable(new PostgresTableSource(operator.data.table, operator.data.columnNames: _*)).asInstanceOf[DataQuanta[Any]]
  }

  //
  // Output operators
  //

  private def visit(operator: TextFileOutputFromJson, dataQuanta: DataQuanta[Any]): DataQuanta[Any] = {
    dataQuanta.writeTextFile(operator.data.filename, (x: Any) => x.toString)
    dataQuanta
  }

  //
  // Unary operators
  //

  private def visit(operator: MapOperatorFromJson, dataQuanta: DataQuanta[Any]): DataQuanta[Any] = {
    dataQuanta.map(ParseScalaFromString.parseLambda(operator.data.udf))
  }

  private def visit(operator: FilterOperatorFromJson, dataQuanta: DataQuanta[Any]): DataQuanta[Any] = {
    dataQuanta.filter(ParseScalaFromString.parseLambda2[Any, Boolean](operator.data.udf))
  }

  private def visit(operator: FlatMapOperatorFromJson, dataQuanta: DataQuanta[Any]): DataQuanta[Any] = {
    dataQuanta.flatMap(ParseScalaFromString.parseLambda2[Any, Iterable[Any]](operator.data.udf))
  }

  private def visit(operator: ReduceByOperatorFromJson, dataQuanta: DataQuanta[Any]): DataQuanta[Any] = {
    dataQuanta.reduceByKey(
      ParseScalaFromString.parseLambda(operator.data.keyUdf),
      ParseScalaFromString.parseLambdaTuple2ToAny(operator.data.udf)
    )
  }

  private def visit(operator: CountOperatorFromJson, dataQuanta: DataQuanta[Any]): DataQuanta[Any] = {
    dataQuanta.count.asInstanceOf[DataQuanta[Any]]
  }

  private def visit(operator: GroupByOpeartorFromJson, dataQuanta: DataQuanta[Any]): DataQuanta[Any] = {
    dataQuanta.groupByKey(ParseScalaFromString.parseLambda(operator.data.keyUdf))
      .map(arrayList => arrayList.asScala.toList)
  }

  private def visit(operator: SortOperatorFromJson, dataQuanta: DataQuanta[Any]): DataQuanta[Any] = {
    dataQuanta.sort(ParseScalaFromString.parseLambda(operator.data.keyUdf))
  }

  private def visit(operator: DistinctOperatorFromJson, dataQuanta: DataQuanta[Any]): DataQuanta[Any] = {
    dataQuanta.distinct
  }

  private def visit(operator: ReduceOperatorFromJson, dataQuanta: DataQuanta[Any]): DataQuanta[Any] = {
    dataQuanta.reduce(ParseScalaFromString.parseLambdaTuple2ToAny(operator.data.udf))
  }

  private def visit(operator: SampleOperatorFromJson, dataQuanta: DataQuanta[Any]): DataQuanta[Any] = {
    dataQuanta.sample(operator.data.sampleSize)
  }

  private def visit(operator: MapPartitionsOperatorFromJson, dataQuanta: DataQuanta[Any]): DataQuanta[Any] = {
    dataQuanta.mapPartitions(ParseScalaFromString.parseLambda2[Iterable[Any], Iterable[Any]](operator.data.udf))
  }

  //
  // Binary operators
  //

  private def visit(operator: UnionOperatorFromJson, dataQuanta1: DataQuanta[Any],  dataQuanta2: DataQuanta[Any]): DataQuanta[Any] = {
    dataQuanta1.union(dataQuanta2)
  }

  private def visit(operator: JoinOperatorFromJson, dataQuanta1: DataQuanta[Any], dataQuanta2: DataQuanta[Any]): DataQuanta[Any] = {
    dataQuanta1.join(
      ParseScalaFromString.parseLambda(operator.data.thisKeyUdf),
      dataQuanta2,
      ParseScalaFromString.parseLambda(operator.data.thatKeyUdf)
    )
      .map(tuple2 => (tuple2.field0, tuple2.field1))
  }

  private def visit(operator: CartesianOperatorFromJson, dataQuanta1: DataQuanta[Any], dataQuanta2: DataQuanta[Any]): DataQuanta[Any] = {
    dataQuanta1.cartesian(dataQuanta2)
      .map(tuple2 => (tuple2.field0, tuple2.field1))
  }

  private def visit(operator: CoGroupOperatorFromJson, dataQuanta1: DataQuanta[Any], dataQuanta2: DataQuanta[Any]): DataQuanta[Any] = {
    dataQuanta1.coGroup(
      ParseScalaFromString.parseLambda(operator.data.thisKeyUdf),
      dataQuanta2,
      ParseScalaFromString.parseLambda(operator.data.thatKeyUdf)
    )
      .map(tuple2 => (tuple2.field0.asScala.toList, tuple2.field1.asScala.toList))
  }

  private def visit(operator: IntersectOperatorFromJson, dataQuanta1: DataQuanta[Any], dataQuanta2: DataQuanta[Any]): DataQuanta[Any] = {
    dataQuanta1.intersect(dataQuanta2)
  }

  //
  // Loop operators
  //
  private def visit(operator: DoWhileOperatorFromJson, dataQuanta: DataQuanta[Any]): DataQuanta[Any] = {
    dataQuanta.doWhile(
      ParseScalaFromString.parseLambda2[Any, Boolean](operator.data.udf),
      ParseScalaFromString.parseLambda2ImportDataQuanta[DataQuanta[Any], (DataQuanta[Any], DataQuanta[Any])](operator.data.bodyBuilder))
  }

  private def visit(operator: RepeatOperatorFromJson, dataQuanta: DataQuanta[Any]): DataQuanta[Any] = {
    dataQuanta.repeat(
      operator.data.n,
      ParseScalaFromString.parseLambda2ImportDataQuanta[DataQuanta[Any], DataQuanta[Any]](operator.data.bodyBuilder))
  }

  private def visit(operator: ForeachOperatorFromJson, dataQuanta: DataQuanta[Any]): DataQuanta[Any] = {
    dataQuanta.foreach(ParseScalaFromString.parseLambda(operator.data.udf))
    dataQuanta
  }

  //
  // Other
  //

  private def visit(operator: KMeansFromJson, dataQuanta: DataQuanta[Any]): DataQuanta[Any] = {

    val distanceUdf = ParseScalaFromString.parseLambdaTuple2ToAny(operator.data.distanceUdf)
    val sumUdf = ParseScalaFromString.parseLambdaTuple2ToAny(operator.data.sumUdf)
    val divideUdf = ParseScalaFromString.parseLambdaTuple2ToAny(operator.data.divideUdf)
    val initialCentroidsUdf = ParseScalaFromString.parseLambda(operator.data.initialCentroidsUdf)

    class SelectNearestCentroid extends ExtendedSerializableFunction[Any, Any] {

      var centroids: java.util.Collection[(Any, Int)] = _

      override def open(executionCtx: ExecutionContext): Unit = {
        centroids = executionCtx.getBroadcast[(Any, Int)]("centroids")
      }

      override def apply(point: Any): Any = {
        var minDistance = Double.PositiveInfinity
        var nearestCentroidId: Int = -1
        for (centroid <- centroids.asScala) {
          val distance = distanceUdf(point, centroid._1).asInstanceOf[Double]
          if (distance < minDistance) {
            minDistance = distance
            nearestCentroidId = centroid._2
          }
        }
        TaggedPointCounter(point, nearestCentroidId, 1)
      }
    }

    case class TaggedPointCounter(point: Any, centroidId: Int, count: Int = 1) {
      def +(that: TaggedPointCounter): TaggedPointCounter =
        TaggedPointCounter(
          sumUdf(this.point, that.point),
          this.centroidId,
          this.count + that.count
        )

      def average: (Any, Int) = (divideUdf(point, count), centroidId)
    }

    val initialCentroids = planBuilder.loadCollection(initialCentroidsUdf(operator.data.k).asInstanceOf[Iterable[Any]])

    val finalCentroids = initialCentroids.map(_.asInstanceOf[(Any, Int)]).repeat(
      operator.data.maxIterations,
      { currentCentroids =>
        dataQuanta
          .mapJava(
            new SelectNearestCentroid
            // udfLoad = LoadProfileEstimators.createFromSpecification("wayang.apps.kmeans.udfs.select-centroid.load", configuration)
          )
          .withBroadcast(currentCentroids, "centroids")
          .reduceByKey(_.asInstanceOf[TaggedPointCounter].centroidId, _.asInstanceOf[TaggedPointCounter] + _.asInstanceOf[TaggedPointCounter])
          .withCardinalityEstimator(operator.data.k)
          .map(_.asInstanceOf[TaggedPointCounter].average)
      }
    )

    finalCentroids.asInstanceOf[DataQuanta[Any]]
  }


}
