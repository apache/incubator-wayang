package org.apache.wayang.multicontext

import org.apache.wayang.core.api.Configuration

package object apps {

  def loadConfig(args: Array[String]): (Configuration, Configuration) = {
    if (args.length < 2) {
      println("Loading default configurations.")
      (new Configuration(), new Configuration())
    } else {
      println("Loading custom configurations.")
      (loadConfigFromUrl(args(0)), loadConfigFromUrl(args(1)))
    }
  }

  private def loadConfigFromUrl(url: String): Configuration = {
    try {
      new Configuration(url)
    } catch {
      case unexpected: Exception =>
        unexpected.printStackTrace()
        println(s"Can't load configuration from $url")
        new Configuration()
    }
  }

}
