package org.qcri.rheem.api

import org.qcri.rheem.core.api.RheemContext

import scala.language.implicitConversions

object Rheem {

  private[rheem] lazy val rheemContext = new RheemContext

  def buildNewPlan = new PlanBuilder(this.rheemContext)

}
