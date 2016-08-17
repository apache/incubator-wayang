package org.qcri.rheem.api

import java.util.{Collection => JavaCollection}

import org.qcri.rheem.core.api.RheemContext
import org.qcri.rheem.core.plan.rheemplan.RheemPlan

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
  def loadCollection[T](collection: JavaCollection[T]) = new LoadCollectionDataQuantaBuilder[T](collection, this)

}
