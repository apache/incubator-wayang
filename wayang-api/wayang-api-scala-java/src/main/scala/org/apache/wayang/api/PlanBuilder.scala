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

package org.apache.wayang.api
/**
 * TODO: add unitary test to the elements in the file org.apache.wayang.api.PlanBuilder.scala
 * labels: unitary-test,todo
 */
import org.apache.commons.lang3.Validate
import org.apache.wayang.api
import org.apache.wayang.basic.data.Record
import org.apache.wayang.basic.operators.{CollectionSource, ObjectFileSource, TableSource, TextFileSource}
import org.apache.wayang.commons.util.profiledb.model.Experiment
import org.apache.wayang.core.api.WayangContext
import org.apache.wayang.core.plan.wayangplan._
import org.apache.wayang.core.util.ReflectionUtils

import scala.collection.JavaConversions
import scala.collection.mutable.ListBuffer
import scala.language.implicitConversions
import scala.reflect._

/**
  * Utility to build [[WayangPlan]]s.
  */
class PlanBuilder(wayangContext: WayangContext, private var jobName: String = null) {

  private[api] val sinks = ListBuffer[Operator]()

  private val udfJars = scala.collection.mutable.Set[String]()

  private var experiment: Experiment = _

  // We need to ensure that this module is shipped to Spark etc. in particular because of the Scala-to-Java function wrappers.
  ReflectionUtils.getDeclaringJar(this) match {
    case path: String => udfJars += path
    case _ =>
  }


  /**
    * Defines user-code JAR files that might be needed to transfer to execution platforms.
    *
    * @param paths paths to JAR files that should be transferred
    * @return this instance
    */
  def withUdfJars(paths: String*) = {
    this.udfJars ++= paths
    this
  }

  /**
    * Defines the [[Experiment]] that should collects metrics of the [[WayangPlan]].
    *
    * @param experiment the [[Experiment]]
    * @return this instance
    */
  def withExperiment(experiment: Experiment) = {
    this.experiment = experiment
    this
  }


  /**
    * Defines the name for the [[WayangPlan]] that is being created.
    *
    * @param jobName the name
    * @return this instance
    */
  def withJobName(jobName: String) = {
    this.jobName = jobName
    this
  }

  /**
    * Defines user-code JAR files that might be needed to transfer to execution platforms.
    *
    * @param classes whose JAR files should be transferred
    * @return this instance
    */
  def withUdfJarsOf(classes: Class[_]*) =
  withUdfJars(classes.map(ReflectionUtils.getDeclaringJar).filterNot(_ == null): _*)


  /**
    * Build the [[org.apache.wayang.core.api.Job]] and execute it.
    */
  def buildAndExecute(): Unit = {
    val plan: WayangPlan = new WayangPlan(this.sinks.toArray: _*)
    if (this.experiment == null) this.wayangContext.execute(jobName, plan, this.udfJars.toArray: _*)
    else this.wayangContext.execute(jobName, plan, this.experiment, this.udfJars.toArray: _*)
  }

  /**
    * Read a text file and provide it as a dataset of [[String]]s, one per line.
    *
    * @param url the URL of the text file
    * @return [[DataQuanta]] representing the file
    */
  def readTextFile(url: String): DataQuanta[String] = load(new TextFileSource(url))


  /**
   * Read a object's file and provide it as a dataset of [[Object]]s.
   *
   * @param url the URL of the Object's file
   * @return [[DataQuanta]] representing the file
   */
  def readObjectFile[T: ClassTag](url: String): DataQuanta[T] = load(new ObjectFileSource(url, dataSetType[T]))

  /**
    * Reads a database table and provides them as a dataset of [[Record]]s.
    *
    * @param source from that the [[Record]]s should be read
    * @return [[DataQuanta]] of [[Record]]s in the table
    */
  def readTable(source: TableSource): DataQuanta[Record] = load(source)

  /**
    * Loads a [[java.util.Collection]] into Wayang and represents them as [[DataQuanta]].
    *
    * @param collection to be loaded
    * @return [[DataQuanta]] the `collection`
    */
  def loadCollection[T: ClassTag](collection: java.util.Collection[T]): DataQuanta[T] =
  load(new CollectionSource[T](collection, dataSetType[T]))

  /**
    * Loads a [[Iterable]] into Wayang and represents it as [[DataQuanta]].
    *
    * @param iterable to be loaded
    * @return [[DataQuanta]] the `iterable`
    */
  def loadCollection[T: ClassTag](iterable: Iterable[T]): DataQuanta[T] =
  loadCollection(JavaConversions.asJavaCollection(iterable))

  /**
    * Load [[DataQuanta]] from an arbitrary [[UnarySource]].
    *
    * @param source that should be loaded from
    * @return the [[DataQuanta]]
    */
  def load[T: ClassTag](source: UnarySource[T]): DataQuanta[T] = wrap(source)

  /**
    * Execute a custom [[Operator]].
    *
    * @param operator that should be executed
    * @param inputs   the input [[DataQuanta]] of the `operator`, aligned with its [[InputSlot]]s
    * @return an [[IndexedSeq]] of the `operator`s output [[DataQuanta]], aligned with its [[OutputSlot]]s
    */
  def customOperator(operator: Operator, inputs: DataQuanta[_]*): IndexedSeq[DataQuanta[_]] = {
    Validate.isTrue(operator.getNumRegularInputs == inputs.size)

    // Set up inputs.
    inputs.zipWithIndex.foreach(zipped => zipped._1.connectTo(operator, zipped._2))

    // Set up outputs.
    for (outputIndex <- 0 until operator.getNumOutputs) yield DataQuanta.create(operator.getOutput(outputIndex))(this)
  }

  implicit private[api] def wrap[T: ClassTag](operator: ElementaryOperator): DataQuanta[T] =
    PlanBuilder.wrap[T](operator)(classTag[T], this)

}

object PlanBuilder {

  implicit private[api] def wrap[T: ClassTag](operator: ElementaryOperator)(implicit planBuilder: PlanBuilder): DataQuanta[T] =
    api.wrap[T](operator)

}
