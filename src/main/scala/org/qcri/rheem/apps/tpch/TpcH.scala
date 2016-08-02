package org.qcri.rheem.apps.tpch

import org.qcri.rheem.apps.tpch.queries.{Query3File, Query3Hybrid, Query3Sqlite}
import org.qcri.rheem.apps.util.{Parameters, StdOut}
import org.qcri.rheem.core.api.Configuration

/**
  * This app adapts some TPC-H queries.
  */
object TpcH {

  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      println("Usage: <main class> <plugin(,plugin)*> <TPC-H config URL> <query> [<query args>*]")
      sys.exit(1)
    }

    val plugins = Parameters.loadPlugins(args(0))
    val configUrl = args(1)
    val queryName = args(2)

    val configuration = new Configuration
    configuration.load(configUrl)

    queryName match {
      case "Query3File" => {
        val query = new Query3File(plugins: _*)
        val result = query(configuration)
        StdOut.printLimited(result, 10)
      }
      case "Query3Sqlite" => {
        val query = new Query3Sqlite(plugins: _*)
        val result = query(configuration)
        StdOut.printLimited(result, 10)
      }
      case "Query3Hybrid" => {
        val query = new Query3Hybrid(plugins: _*)
        val result = query(configuration)
        StdOut.printLimited(result, 10)
      }
      case other: String => {
        println(s"Unknown query: $other")
        sys.exit(1)
      }
    }
  }

}
