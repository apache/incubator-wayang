package org.qcri.rheem.api

import java.util

import org.qcri.rheem.core.api.RheemContext
import org.qcri.rheem.core.plan.rheemplan.RheemPlan

/**
  *  Utility to build [[RheemPlan]]s.
  */
class JavaPlanBuilder(rheemCtx: RheemContext) {

  protected[api] val planBuilder = new PlanBuilder(rheemCtx)

  def loadCollection[T](collection: util.Collection[T]) = new LoadCollectionDataQuantaBuilder[T](collection, this)

}
