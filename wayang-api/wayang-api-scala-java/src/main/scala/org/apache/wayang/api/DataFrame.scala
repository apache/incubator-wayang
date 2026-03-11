package org.apache.wayang.api

import org.apache.wayang.basic.operators.SelectOperator

/**
 * DataFrame abstraction for Apache Wayang, specializing DataQuanta for Row types.
 * The idea is the following: DataQuanta[] is a good abstraction for both typed and untyped data structures.
 * Taking spark as example, DQ can abstract both hard-typed JavaRdd (e.g. DataQuanta[Person]) and an untyped Dataset[Row] (i.e. DataFrame)
 * (so, DataQuanta[Row]).
 * For this reason it is possible for a Wayang DataFrame to be a wrapper around a DataQuanta[Row].
 * Row's core is list of untyped (Any) elements and a schema
 * that allows to associate names of columns to both elements of row and their actual type.
 * Taking Spark as example, a DataQuanta[Row] is translated into a Dataset[Row].
 */
class DataFrame(df: DataQuanta[Row]) {

  /**
   * Selects specific columns based on the provided input strings.
   * @return A new DataFrame containing only the selected columns.
   */
  def select(columns: String*): DataFrame = {
    val selectOperator = new SelectOperator(columns)
    this.df.connectTo(selectOperator, 0)
    implicit val pb: PlanBuilder = df.planBuilder
    val dq =new DataQuanta[Row](selectOperator)
    new DataFrame(dq)
  }

}