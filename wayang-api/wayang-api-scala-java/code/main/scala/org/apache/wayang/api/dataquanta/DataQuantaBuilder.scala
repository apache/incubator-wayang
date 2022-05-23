/*
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an
 *   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *   KIND, either express or implied.  See the License for the
 *   specific language governing permissions and limitations
 *   under the License.
 */

package org.apache.wayang.api.dataquanta

import org.apache.wayang.api.dataquanta.builder.{CartesianDataQuantaBuilder, CoGroupDataQuantaBuilder, CountDataQuantaBuilder, CustomOperatorDataQuantaBuilder, DistinctDataQuantaBuilder, DoWhileDataQuantaBuilder, FilterDataQuantaBuilder, FlatMapDataQuantaBuilder, GlobalGroupDataQuantaBuilder, GlobalReduceDataQuantaBuilder, GroupByDataQuantaBuilder, IntersectDataQuantaBuilder, JoinDataQuantaBuilder, KeyedDataQuantaBuilder, MapDataQuantaBuilder, MapPartitionsDataQuantaBuilder, ProjectionDataQuantaBuilder, ReduceByDataQuantaBuilder, RepeatDataQuantaBuilder, SampleDataQuantaBuilder, SortDataQuantaBuilder, UnionDataQuantaBuilder, ZipWithIdDataQuantaBuilder}
import org.apache.wayang.api.graph.{Edge, EdgeDataQuantaBuilder, EdgeDataQuantaBuilderDecorator}
import org.apache.wayang.api.util.{DataQuantaBuilderCache, TypeTrap}
import org.apache.wayang.api.{JavaPlanBuilder, RecordDataQuantaBuilder, RecordDataQuantaBuilderDecorator}
import org.apache.wayang.basic.data.Record
import org.apache.wayang.basic.operators.{GlobalReduceOperator, LocalCallbackSink, MapOperator}
import org.apache.wayang.core.function.FunctionDescriptor.{SerializableBinaryOperator, SerializableFunction, SerializablePredicate}
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimator
import org.apache.wayang.core.plan.wayangplan.{Operator, OutputSlot, WayangPlan}
import org.apache.wayang.core.platform.Platform
import org.apache.wayang.core.types.DataSetType
import org.apache.wayang.core.util.Logging
import org.apache.wayang.core.util.{Tuple => WayangTuple}
import org.apache.wayang.commons.util.profiledb.model.Experiment

import java.util.function.{Consumer, IntUnaryOperator, Function => JavaFunction}
import java.util.{Collection => JavaCollection}
import scala.reflect.ClassTag

/**
 * Trait/interface for builders of [[DataQuanta]]. The purpose of the builders is to provide a convenient
 * Java API for Wayang that compensates for lacking default and named arguments.
 */
trait DataQuantaBuilder[+This <: DataQuantaBuilder[_, Out], Out] extends Logging {

  /**
   * The type of the [[DataQuanta]] to be built.
   */
  protected[api] def outputTypeTrap: TypeTrap

  /**
   * Provide a [[JavaPlanBuilder]] to which this instance is associated.
   */
  protected[api] implicit def javaPlanBuilder: JavaPlanBuilder

  /**
   * Set a name for the [[DataQuanta]] and its associated [[org.apache.wayang.core.plan.wayangplan.Operator]]s.
   *
   * @param name the name
   * @return this instance
   */
  def withName(name: String): This

  /**
   * Set an [[Experiment]] for the currently built [[org.apache.wayang.core.api.Job]].
   *
   * @param experiment the [[Experiment]]
   * @return this instance
   */
  def withExperiment(experiment: Experiment): This

  /**
   * Explicitly set an output [[DataSetType]] for the currently built [[DataQuanta]]. Note that it is not
   * always necessary to set it and that it can be inferred in some situations.
   *
   * @param outputType the output [[DataSetType]]
   * @return this instance
   */
  def withOutputType(outputType: DataSetType[Out]): This

  /**
   * Explicitly set an output [[Class]] for the currently built [[DataQuanta]]. Note that it is not
   * always necessary to set it and that it can be inferred in some situations.
   *
   * @param cls the output [[Class]]
   * @return this instance
   */
  def withOutputClass(cls: Class[Out]): This

  /**
   * Register a broadcast with the [[DataQuanta]] to be built
   *
   * @param sender        a [[DataQuantaBuilder]] constructing the broadcasted [[DataQuanta]]
   * @param broadcastName the name of the broadcast
   * @return this instance
   */
  def withBroadcast[Sender <: DataQuantaBuilder[_, _]](sender: Sender, broadcastName: String): This

  /**
   * Set a [[CardinalityEstimator]] for the currently built [[DataQuanta]].
   *
   * @param cardinalityEstimator the [[CardinalityEstimator]]
   * @return this instance
   */
  def withCardinalityEstimator(cardinalityEstimator: CardinalityEstimator): This

  /**
   * Add a target [[Platform]] on which the currently built [[DataQuanta]] should be calculated. Can be invoked
   * multiple times to set multiple possilbe target [[Platform]]s or not at all to impose no restrictions.
   *
   * @param platform the [[CardinalityEstimator]]
   * @return this instance
   */
  def withTargetPlatform(platform: Platform): This

  /**
   * Register the JAR file containing the given [[Class]] with the currently built [[org.apache.wayang.core.api.Job]].
   *
   * @param cls the [[Class]]
   * @return this instance
   */
  def withUdfJarOf(cls: Class[_]): This

  /**
   * Register a JAR file with the currently built [[org.apache.wayang.core.api.Job]].
   *
   * @param path the path of the JAR file
   * @return this instance
   */
  def withUdfJar(path: String): This

  /**
   * Provide a [[ClassTag]] for the constructed [[DataQuanta]].
   *
   * @return the [[ClassTag]]
   */
  protected[api] implicit def classTag: ClassTag[Out] = ClassTag(outputTypeTrap.typeClass)

  /**
   * Feed the built [[DataQuanta]] into a [[MapOperator]].
   *
   * @param udf the UDF for the [[MapOperator]]
   * @return a [[MapDataQuantaBuilder]]
   */
  def map[NewOut](udf: SerializableFunction[Out, NewOut]) = new MapDataQuantaBuilder(this, udf)

  /**
   * Feed the built [[DataQuanta]] into a [[MapOperator]] with a [[org.apache.wayang.basic.function.ProjectionDescriptor]].
   *
   * @param fieldNames field names for the [[org.apache.wayang.basic.function.ProjectionDescriptor]]
   * @return a [[MapDataQuantaBuilder]]
   */
  def project[NewOut](fieldNames: Array[String]) = new ProjectionDataQuantaBuilder(this, fieldNames)

  /**
   * Feed the built [[DataQuanta]] into a [[org.apache.wayang.basic.operators.FilterOperator]].
   *
   * @param udf filter UDF
   * @return a [[FilterDataQuantaBuilder]]
   */
  def filter(udf: SerializablePredicate[Out]) = new FilterDataQuantaBuilder(this, udf)

  /**
   * Feed the built [[DataQuanta]] into a [[org.apache.wayang.basic.operators.FlatMapOperator]].
   *
   * @param udf the UDF for the [[org.apache.wayang.basic.operators.FlatMapOperator]]
   * @return a [[FlatMapDataQuantaBuilder]]
   */
  def flatMap[NewOut](udf: SerializableFunction[Out, java.lang.Iterable[NewOut]]) = new FlatMapDataQuantaBuilder(this, udf)

  /**
   * Feed the built [[DataQuanta]] into a [[org.apache.wayang.basic.operators.MapPartitionsOperator]].
   *
   * @param udf the UDF for the [[org.apache.wayang.basic.operators.MapPartitionsOperator]]
   * @return a [[MapPartitionsDataQuantaBuilder]]
   */
  def mapPartitions[NewOut](udf: SerializableFunction[java.lang.Iterable[Out], java.lang.Iterable[NewOut]]) =
    new MapPartitionsDataQuantaBuilder(this, udf)

  /**
   * Feed the built [[DataQuanta]] into a [[org.apache.wayang.basic.operators.SampleOperator]].
   *
   * @param sampleSize the absolute size of the sample
   * @return a [[SampleDataQuantaBuilder]]
   */
  def sample(sampleSize: Int): SampleDataQuantaBuilder[Out] = this.sample(new IntUnaryOperator {
    override def applyAsInt(operand: Int): Int = sampleSize
  })


  /**
   * Feed the built [[DataQuanta]] into a [[org.apache.wayang.basic.operators.SampleOperator]].
   *
   * @param sampleSizeFunction the absolute size of the sample as a function of the current iteration number
   * @return a [[SampleDataQuantaBuilder]]
   */
  def sample(sampleSizeFunction: IntUnaryOperator) = new SampleDataQuantaBuilder[Out](this, sampleSizeFunction)

  /**
   * Annotates a key to this instance.
   *
   * @param keyExtractor extracts the key from the data quanta
   * @return a [[KeyedDataQuantaBuilder]]
   */
  def keyBy[Key](keyExtractor: SerializableFunction[Out, Key]) = new KeyedDataQuantaBuilder[Out, Key](this, keyExtractor)

  /**
   * Feed the built [[DataQuanta]] into a [[GlobalReduceOperator]].
   *
   * @param udf the UDF for the [[GlobalReduceOperator]]
   * @return a [[GlobalReduceDataQuantaBuilder]]
   */
  def reduce(udf: SerializableBinaryOperator[Out]) = new GlobalReduceDataQuantaBuilder(this, udf)

  /**
   * Feed the built [[DataQuanta]] into a [[org.apache.wayang.basic.operators.ReduceByOperator]].
   *
   * @param keyUdf the key UDF for the [[org.apache.wayang.basic.operators.ReduceByOperator]]
   * @param udf    the UDF for the [[org.apache.wayang.basic.operators.ReduceByOperator]]
   * @return a [[ReduceByDataQuantaBuilder]]
   */
  def reduceByKey[Key](keyUdf: SerializableFunction[Out, Key], udf: SerializableBinaryOperator[Out]) =
    new ReduceByDataQuantaBuilder(this, keyUdf, udf)

  /**
   * Feed the built [[DataQuanta]] into a [[org.apache.wayang.basic.operators.MaterializedGroupByOperator]].
   *
   * @param keyUdf the key UDF for the [[org.apache.wayang.basic.operators.MaterializedGroupByOperator]]
   * @return a [[GroupByDataQuantaBuilder]]
   */
  def groupByKey[Key](keyUdf: SerializableFunction[Out, Key]) =
    new GroupByDataQuantaBuilder(this, keyUdf)

  /**
   * Feed the built [[DataQuanta]] into a [[org.apache.wayang.basic.operators.GlobalMaterializedGroupOperator]].
   *
   * @return a [[GlobalGroupDataQuantaBuilder]]
   */
  def group() = new GlobalGroupDataQuantaBuilder(this)

  /**
   * Feed the built [[DataQuanta]] of this and the given instance into a
   * [[org.apache.wayang.basic.operators.UnionAllOperator]].
   *
   * @param that the other [[DataQuantaBuilder]] to union with
   * @return a [[UnionDataQuantaBuilder]]
   */
  def union(that: DataQuantaBuilder[_, Out]) = new UnionDataQuantaBuilder(this, that)

  /**
   * Feed the built [[DataQuanta]] of this and the given instance into a
   * [[org.apache.wayang.basic.operators.IntersectOperator]].
   *
   * @param that the other [[DataQuantaBuilder]] to intersect with
   * @return an [[IntersectDataQuantaBuilder]]
   */
  def intersect(that: DataQuantaBuilder[_, Out]) = new IntersectDataQuantaBuilder(this, that)

  /**
   * Feed the built [[DataQuanta]] of this and the given instance into a
   * [[org.apache.wayang.basic.operators.JoinOperator]].
   *
   * @param thisKeyUdf the key extraction UDF for this instance
   * @param that       the other [[DataQuantaBuilder]] to join with
   * @param thatKeyUdf the key extraction UDF for `that` instance
   * @return a [[JoinDataQuantaBuilder]]
   */
  def join[ThatOut, Key](thisKeyUdf: SerializableFunction[Out, Key],
                         that: DataQuantaBuilder[_, ThatOut],
                         thatKeyUdf: SerializableFunction[ThatOut, Key]) =
    new JoinDataQuantaBuilder(this, that, thisKeyUdf, thatKeyUdf)

  /**
   * Feed the built [[DataQuanta]] of this and the given instance into a
   * [[org.apache.wayang.basic.operators.CoGroupOperator]].
   *
   * @param thisKeyUdf the key extraction UDF for this instance
   * @param that       the other [[DataQuantaBuilder]] to join with
   * @param thatKeyUdf the key extraction UDF for `that` instance
   * @return a [[CoGroupDataQuantaBuilder]]
   */
  def coGroup[ThatOut, Key](thisKeyUdf: SerializableFunction[Out, Key],
                            that: DataQuantaBuilder[_, ThatOut],
                            thatKeyUdf: SerializableFunction[ThatOut, Key]) =
    new CoGroupDataQuantaBuilder(this, that, thisKeyUdf, thatKeyUdf)


  /**
   * Feed the built [[DataQuanta]] of this and the given instance into a
   * [[org.apache.wayang.basic.operators.SortOperator]].
   *
   * @param keyUdf the key extraction UDF for this instance
   * @return a [[SortDataQuantaBuilder]]
   */
  def sort[Key](keyUdf: SerializableFunction[Out, Key]) =
    new SortDataQuantaBuilder(this, keyUdf)

  /**
   * Feed the built [[DataQuanta]] of this and the given instance into a
   * [[org.apache.wayang.basic.operators.CartesianOperator]].
   *
   * @return a [[CartesianDataQuantaBuilder]]
   */
  def cartesian[ThatOut](that: DataQuantaBuilder[_, ThatOut]) = new CartesianDataQuantaBuilder(this, that)

  /**
   * Feed the built [[DataQuanta]] into a [[org.apache.wayang.basic.operators.ZipWithIdOperator]].
   *
   * @return a [[ZipWithIdDataQuantaBuilder]] representing the [[org.apache.wayang.basic.operators.ZipWithIdOperator]]'s output
   */
  def zipWithId = new ZipWithIdDataQuantaBuilder(this)

  /**
   * Feed the built [[DataQuanta]] into a [[org.apache.wayang.basic.operators.DistinctOperator]].
   *
   * @return a [[DistinctDataQuantaBuilder]] representing the [[org.apache.wayang.basic.operators.DistinctOperator]]'s output
   */
  def distinct = new DistinctDataQuantaBuilder(this)

  /**
   * Feed the built [[DataQuanta]] into a [[org.apache.wayang.basic.operators.CountOperator]].
   *
   * @return a [[CountDataQuantaBuilder]] representing the [[org.apache.wayang.basic.operators.CountOperator]]'s output
   */
  def count = new CountDataQuantaBuilder(this)

  /**
   * Feed the built [[DataQuanta]] into a [[org.apache.wayang.basic.operators.DoWhileOperator]].
   *
   * @return a [[DoWhileDataQuantaBuilder]]
   */
  def doWhile[Conv](conditionUdf: SerializablePredicate[JavaCollection[Conv]],
                    bodyBuilder: JavaFunction[DataQuantaBuilder[_, Out], WayangTuple[DataQuantaBuilder[_, Out], DataQuantaBuilder[_, Conv]]]) =
    new DoWhileDataQuantaBuilder(this, conditionUdf.asInstanceOf[SerializablePredicate[JavaCollection[Conv]]], bodyBuilder)

  /**
   * Feed the built [[DataQuanta]] into a [[org.apache.wayang.basic.operators.RepeatOperator]].
   *
   * @return a [[DoWhileDataQuantaBuilder]]
   */
  def repeat(numRepetitions: Int, bodyBuilder: JavaFunction[DataQuantaBuilder[_, Out], DataQuantaBuilder[_, Out]]) =
    new RepeatDataQuantaBuilder(this, numRepetitions, bodyBuilder)

  /**
   * Feed the built [[DataQuanta]] into a custom [[Operator]] with a single [[org.apache.wayang.core.plan.wayangplan.InputSlot]]
   * and a single [[OutputSlot]].
   *
   * @param operator the custom [[Operator]]
   * @tparam T the type of the output [[DataQuanta]]
   * @return a [[CustomOperatorDataQuantaBuilder]]
   */
  def customOperator[T](operator: Operator) = {
    assert(operator.getNumInputs == 1, "customOperator(...) only allows for operators with a single input.")
    assert(operator.getNumOutputs == 1, "customOperator(...) only allows for operators with a single output.")
    new CustomOperatorDataQuantaBuilder[T](operator, 0, new DataQuantaBuilderCache, this)
  }

  /**
   * Feed the built [[DataQuanta]] into a [[LocalCallbackSink]] that collects all data quanta locally. This triggers
   * execution of the constructed [[WayangPlan]].
   *
   * @return the collected data quanta
   */
  def collect(): JavaCollection[Out] = {
    import scala.collection.JavaConversions._
    this.dataQuanta().collect()
  }

  /**
   * Feed the built [[DataQuanta]] into a [[JavaFunction]] that runs locally. This triggers
   * execution of the constructed [[WayangPlan]].
   *
   * @param f the [[JavaFunction]]
   * @return the collected data quanta
   */
  def forEach(f: Consumer[Out]): Unit = this.dataQuanta().foreachJava(f)

  /**
   * Feed the built [[DataQuanta]] into a [[org.apache.wayang.basic.operators.TextFileSink]]. This triggers
   * execution of the constructed [[WayangPlan]].
   *
   * @param url     the URL of the file to be written
   * @param jobName optional name for the [[WayangPlan]]
   * @return the collected data quanta
   */
  def writeTextFile(url: String, formatterUdf: SerializableFunction[Out, String], jobName: String): Unit =
    this.writeTextFile(url, formatterUdf, jobName, null)

  /**
   * Feed the built [[DataQuanta]] into a [[org.apache.wayang.basic.operators.TextFileSink]]. This triggers
   * execution of the constructed [[WayangPlan]].
   *
   * @param url the URL of the file to be written
   * @return the collected data quanta
   */
  def writeTextFile(url: String,
                    formatterUdf: SerializableFunction[Out, String],
                    jobName: String,
                    udfLoadProfileEstimator: LoadProfileEstimator): Unit = {
    this.javaPlanBuilder.withJobName(jobName)
    this.dataQuanta().writeTextFileJava(url, formatterUdf, udfLoadProfileEstimator)
  }

  /**
   * Enriches the set of operations to [[Record]]-based ones. This instances must deal with data quanta of
   * type [[Record]], though. Because of Java's type erasure, we need to leave it up to you whether this
   * operation is applicable.
   *
   * @return a [[RecordDataQuantaBuilder]]
   */
  def asRecords[T <: RecordDataQuantaBuilder[T]]: RecordDataQuantaBuilder[T] = {
    this match {
      case records: RecordDataQuantaBuilder[_] => records.asInstanceOf[RecordDataQuantaBuilder[T]]
      case _ => new RecordDataQuantaBuilderDecorator(this.asInstanceOf[DataQuantaBuilder[_, Record]])
    }
  }

  /**
   * Enriches the set of operations to [[Edge]]-based ones. This instances must deal with data quanta of
   * type [[Edge]], though. Because of Java's type erasure, we need to leave it up to you whether this
   * operation is applicable.
   *
   * @return a [[EdgeDataQuantaBuilder]]
   */
  def asEdges[T <: EdgeDataQuantaBuilder[T]]: EdgeDataQuantaBuilder[T] = {
    this match {
      case edges: RecordDataQuantaBuilder[_] => edges.asInstanceOf[EdgeDataQuantaBuilder[T]]
      case _ => new EdgeDataQuantaBuilderDecorator(this.asInstanceOf[DataQuantaBuilder[_, Edge]])
    }
  }

  /**
   * Get or create the [[DataQuanta]] built by this instance.
   *
   * @return the [[DataQuanta]]
   */
  protected[api] def dataQuanta(): DataQuanta[Out]

}
