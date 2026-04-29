package org.apache.wayang.api

import org.apache.wayang.basic.data.Record
import org.apache.wayang.basic.operators.SelectOperator
import org.apache.wayang.core.plan.wayangplan.ElementaryOperator

import java.util.{Arrays, ArrayList => JArrayList}

/**
 * DataFrame abstraction for Apache Wayang, specializing DataQuanta for [[Record]](s).
 * The idea is the following: DataQuanta[] is a good abstraction for both hard-typed structures and Dataframes.
 * Taking Wayang-Spark as example, DataQuanta can abstract both JavaRdd (DataQuanta[Person]);
 * and Dataset[Row] i.e. DataFrame (DataQuanta[Record]).
 *
 * For this reason, it is possible for a Wayang DataFrame to be a wrapper around a DataQuanta[[Record]].
 * DataFrame API will provide methods that take expressions as input instead of udf. This allows the new API
 * to leverage modern engines (e.g. Spark Dataframe) with their advanced optimizations (e.g. Predicate Pushdown).
 *
 * In this draft, DataFrame extends DataQuanta allowing the user to call Dataframe-style methods.
 *
 */
class DataFrame private (operator: ElementaryOperator)(implicit planBuilder: PlanBuilder)
  extends DataQuanta[Record](operator) {

  /**
   * Selects specific columns and returns a new DataFrame.
   */
  def select(columns: String*): DataFrame = {
    val cols = new JArrayList[String](Arrays.asList(columns: _*))
    val selectOperator = new SelectOperator(cols)
    this.connectTo(selectOperator, 0)
    new DataFrame(selectOperator)
  }
}