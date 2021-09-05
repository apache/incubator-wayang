package org.qcri.rheem.apps.simwords

import org.qcri.rheem.core.function.FunctionDescriptor.SerializableFunction

import scala.util.Random

/**
  * Attaches [[String]]s with a random [[Int]].
  */
class AddIdFunction extends SerializableFunction[String, (String, Int)] {

  lazy val random = new Random

  override def apply(t: String): (String, Int) = (t, random.nextInt())

}
