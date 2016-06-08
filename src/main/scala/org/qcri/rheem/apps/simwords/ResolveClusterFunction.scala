package org.qcri.rheem.apps.simwords

import org.qcri.rheem.core.function.ExecutionContext
import org.qcri.rheem.core.function.FunctionDescriptor.ExtendedSerializableFunction

import scala.collection.JavaConversions._

/**
  * This function creates word neighborhood vectors out of a text.
  */
class ResolveClusterFunction(dictionaryBroadcastName: String)
  extends ExtendedSerializableFunction[List[Int], List[String]] {

  private var dictionary: Map[Int, String] = _

  override def open(ctx: ExecutionContext): Unit = {
    this.dictionary = ctx.getBroadcast[(String, Int)](dictionaryBroadcastName).map(_.swap).toMap
  }

  override def apply(ids: List[Int]): List[String] =
    ids.map(id => dictionary.getOrElse(id, "???"))
}
