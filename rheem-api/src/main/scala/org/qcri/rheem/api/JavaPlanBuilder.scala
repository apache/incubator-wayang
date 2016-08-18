package org.qcri.rheem.api

import java.util.{Collection => JavaCollection}

import de.hpi.isg.profiledb.store.model.Experiment
import org.apache.commons.lang3.Validate
import org.qcri.rheem.api.util.DataQuantaBuilderCache
import org.qcri.rheem.basic.data.Record
import org.qcri.rheem.basic.operators.{TableSource, TextFileSource}
import org.qcri.rheem.core.api.RheemContext
import org.qcri.rheem.core.plan.rheemplan._

import scala.reflect.ClassTag

/**
  * Utility to build and execute [[RheemPlan]]s.
  */
class JavaPlanBuilder(rheemCtx: RheemContext, jobName: String) {

  def this(rheemContext: RheemContext) = this(rheemContext, null)

  /**
    * A [[PlanBuilder]] that actually takes care of building [[RheemPlan]]s.
    */
  protected[api] val planBuilder = new PlanBuilder(rheemCtx, jobName = jobName)

  /**
    * Feed a [[JavaCollection]] into a [[org.qcri.rheem.basic.operators.CollectionSource]].
    *
    * @param collection the [[JavaCollection]]
    * @return a [[DataQuantaBuilder]] to further develop and configure the just started [[RheemPlan]]
    */
  def loadCollection[T](collection: JavaCollection[T]) = new LoadCollectionDataQuantaBuilder[T](collection)(this)

  /**
    * Read a text file and provide it as a dataset of [[String]]s, one per line.
    *
    * @param url the URL of the text file
    * @return [[DataQuantaBuilder]] for the file
    */
  def readTextFile(url: String): UnarySourceDataQuantaBuilder[UnarySourceDataQuantaBuilder[_, String], String] =
  load(new TextFileSource(url))

  /**
    * Reads a database table and provides them as a dataset of [[Record]]s.
    *
    * @param source from that the [[Record]]s should be read
    * @return [[DataQuantaBuilder]] for the [[Record]]s in the table
    */
  def readTable(source: TableSource) = load(source).asRecords


  /**
    * Load [[DataQuanta]] from an arbitrary [[UnarySource]].
    *
    * @param source that should be loaded from
    * @return the [[DataQuanta]]
    */
  def load[T: ClassTag](source: UnarySource[T]) = new UnarySourceDataQuantaBuilder[UnarySourceDataQuantaBuilder[_, T], T](source)(this)

  /**
    * Execute a custom [[Operator]].
    *
    * @param operator that should be executed
    * @param inputs   the input [[DataQuanta]] of the `operator`, aligned with its [[InputSlot]]s
    * @return an [[IndexedSeq]] of the `operator`s output [[DataQuanta]], aligned with its [[OutputSlot]]s
    */
  def customOperator(operator: Operator, inputs: Array[DataQuantaBuilder[_, _]]): IndexedSeq[CustomOperatorDataQuantaBuilder[_]] = {
    Validate.isTrue(operator.getNumRegularInputs == inputs.length)

    // Set up inputs and the build cache.
    val buildCache = new DataQuantaBuilderCache

    // Set up outputs.
    for (outputIndex <- 0 until operator.getNumOutputs)
      yield new CustomOperatorDataQuantaBuilder(operator, outputIndex, buildCache, inputs: _*)(this)
  }

  /**
    * Defines user-code JAR files that might be needed to transfer to execution platforms.
    *
    * @param paths paths to JAR files that should be transferred
    * @return this instance
    */
  def withUdfJars(paths: String*) = {
    this.planBuilder withUdfJars (paths: _*)
    this
  }


  /**
    * Defines the [[Experiment]] that should collects metrics of the [[RheemPlan]].
    *
    * @param experiment the [[Experiment]]
    * @return this instance
    */
  def withExperiment(experiment: Experiment) = {
    this.planBuilder withExperiment experiment
    this
  }

  /**
    * Defines user-code JAR files that might be needed to transfer to execution platforms.
    *
    * @param classes whose JAR files should be transferred
    * @return this instance
    */
  def withUdfJarsOf(classes: Class[_]*) = {
    this.planBuilder withUdfJarsOf (classes: _*)
    this
  }

  /**
    * Defines the name for the [[RheemPlan]] that is being created.
    *
    * @param jobName the name
    * @return this instance
    */
  def withJobName(jobName: String) = {
    this.planBuilder withJobName jobName
    this
  }

}