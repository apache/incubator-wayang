package org.qcri.rheem.apps.crocopr

import org.qcri.rheem.core.function.ExecutionContext
import org.qcri.rheem.core.function.FunctionDescriptor.ExtendedSerializableFunction
import org.qcri.rheem.core.util.RheemCollections

/**
  * Creates initial page ranks.
  */
class CreateInitialPageRanks(numPagesBroadcast: String)
  extends ExtendedSerializableFunction[(Long, Iterable[Long]), (Long, Double)] {

  private var initialRank: Double = _

  override def open(executionCtx: ExecutionContext): Unit = {
    val numPages = RheemCollections.getSingle(executionCtx.getBroadcast[Long](numPagesBroadcast))
    this.initialRank = 1d / numPages
  }

  override def apply(in: (Long, Iterable[Long])) = (in._1, this.initialRank)

}
