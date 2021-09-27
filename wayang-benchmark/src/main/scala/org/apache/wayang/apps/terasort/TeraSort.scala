package org.apache.wayang.apps.terasort

import com.google.common.primitives.Longs
import org.apache.wayang.api.PlanBuilder
import org.apache.wayang.commons.util.profiledb.model.Experiment
import org.apache.wayang.core.api.{Configuration, WayangContext}
import org.apache.wayang.core.plugin.Plugin

class TeraSort(@transient plugins: Plugin*) extends Serializable {

  def apply(input_url: String, output_url: String)
           (implicit configuration: Configuration, experiment: Experiment) = {

    val wayangCtx = new WayangContext(configuration)
    plugins.foreach(wayangCtx.register)
    val planBuilder = new PlanBuilder(wayangCtx)

    planBuilder
      .readObjectFile[Tuple2[Array[Byte], Array[Byte]]](input_url)
      .sort(t => {
        val bytes = t._1;
        Longs.fromBytes(0, bytes(0), bytes(1), bytes(2), bytes(3), bytes(4), bytes(5), bytes(6))
      })
      .writeObjectFile(output_url);
  }

}
