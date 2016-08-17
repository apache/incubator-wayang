package org.qcri.rheem.api

import java.util.{Collection => JavaCollection}

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
class JavaPlanBuilder(rheemCtx: RheemContext) {

  /**
    * A [[PlanBuilder]] that actually takes care of building [[RheemPlan]]s.
    */
  protected[api] val planBuilder = new PlanBuilder(rheemCtx)

  /**
    * Feed a [[JavaCollection]] into a [[org.qcri.rheem.basic.operators.CollectionSource]].
    *
    * @param collection the [[JavaCollection]]
    * @tparam T
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
  def readTable(source: TableSource): UnarySourceDataQuantaBuilder[UnarySourceDataQuantaBuilder[_, Record], Record] =
  load(source)

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
    val x = for (outputIndex <- 0 until operator.getNumOutputs)
      yield new CustomOperatorDataQuantaBuilder(operator, outputIndex, buildCache, inputs: _*)(this)
    x
  }

}
