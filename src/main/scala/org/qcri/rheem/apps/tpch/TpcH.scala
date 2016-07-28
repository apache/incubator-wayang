package org.qcri.rheem.apps.tpch

import org.qcri.rheem.apps.tpch.queries.Query3
import org.qcri.rheem.apps.util.{Parameters, StdOut}
import org.qcri.rheem.core.api.Configuration

/**
  * This app adapts some TPC-H queries.
  */
object TpcH {

  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      println("Usage: <main class> <platform(,platform)*> <TPC-H config URL> <query> [<query args>*]")
      sys.exit(1)
    }

    val platforms = Parameters.loadPlatforms(args(0), () => new Configuration)
    val configUrl = args(1)
    val queryName = args(2)

    val configuration = new Configuration
    configuration.load(configUrl)

    queryName match {
      case "Query3" => {
        val query = new Query3(platforms: _*)
        val result = query(configuration)
        StdOut.printLimited(result, 10)
      }
    }
  }

}
