package org.qcri.rheem.apps.tpch

import de.hpi.isg.profiledb.store.model.Experiment
import org.qcri.rheem.apps.tpch.queries.{Query3File, Query3Hybrid, Query3Sqlite}
import org.qcri.rheem.apps.util.{Parameters, ProfileDBHelper, StdOut}
import org.qcri.rheem.core.api.Configuration

/**
  * This app adapts some TPC-H queries.
  */
object TpcH {

  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      println(s"Usage: <main class> ${Parameters.experimentHelp} <plugin(,plugin)*> <TPC-H config URL> <query> [<query args>*]")
      sys.exit(1)
    }

    val experimentArg = args(0)
    val plugins = Parameters.loadPlugins(args(1))
    val configUrl = args(2)
    val queryName = args(3)

    val configuration = new Configuration
    configuration.load(configUrl)

    var experiment: Experiment = null
    queryName match {
      case "Query3File" => {
        val query = new Query3File(plugins: _*)
        experiment = Parameters.createExperiment(experimentArg, query)
        experiment.getSubject.addConfiguration("plugins", args(1))
        experiment.getSubject.addConfiguration("query", args(3))
        val result = query(configuration)(experiment)
        StdOut.printLimited(result, 10)
      }
      case "Query3Sqlite" => {
        val query = new Query3Sqlite(plugins: _*)
        experiment = Parameters.createExperiment(experimentArg, query)
        experiment.getSubject.addConfiguration("plugins", args(1))
        experiment.getSubject.addConfiguration("query", args(3))
        val result = query(configuration)(experiment)
        StdOut.printLimited(result, 10)
      }
      case "Query3Hybrid" => {
        val query = new Query3Hybrid(plugins: _*)
        experiment = Parameters.createExperiment(experimentArg, query)
        experiment.getSubject.addConfiguration("plugins", args(1))
        experiment.getSubject.addConfiguration("query", args(3))
        val result = query(configuration)(experiment)
        StdOut.printLimited(result, 10)
      }
      case other: String => {
        println(s"Unknown query: $other")
        sys.exit(1)
      }
    }

    // Store experiment data.
    ProfileDBHelper.store(experiment, configuration)
  }

}
