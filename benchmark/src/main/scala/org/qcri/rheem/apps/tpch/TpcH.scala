package org.qcri.rheem.apps.tpch

import de.hpi.isg.profiledb.store.model.Experiment
import org.qcri.rheem.apps.tpch.queries.{Query1, Query3Database, Query3File, Query3Hybrid}
import org.qcri.rheem.apps.util.{Parameters, ProfileDBHelper, StdOut}
import org.qcri.rheem.core.api.Configuration
import org.qcri.rheem.jdbc.platform.JdbcPlatformTemplate
import org.qcri.rheem.postgres.Postgres
import org.qcri.rheem.postgres.operators.PostgresTableSource
import org.qcri.rheem.sqlite3.Sqlite3
import org.qcri.rheem.sqlite3.operators.Sqlite3TableSource
import scala.collection.JavaConversions._

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

    val jdbcPlatform = {
      val jdbcPlatforms = plugins
        .flatMap(_.getRequiredPlatforms)
        .filter(_.isInstanceOf[JdbcPlatformTemplate])
        .distinct
      if (jdbcPlatforms.size == 1) jdbcPlatforms.head.asInstanceOf[JdbcPlatformTemplate]
      else if (jdbcPlatforms.isEmpty) null
      else throw new IllegalArgumentException(s"Detected multiple databases: ${jdbcPlatforms.mkString(", ")}.")
    }

    val createTableSource =
      if (jdbcPlatform == null) {
        (table: String, columns: Seq[String]) => throw new IllegalStateException("No database plugin detected.")
      } else if (jdbcPlatform.equals(Sqlite3.platform)) {
        (table: String, columns: Seq[String]) => new Sqlite3TableSource(table, columns: _*)
      } else if (jdbcPlatform.equals(Postgres.platform)) {
        (table: String, columns: Seq[String]) => new PostgresTableSource(table, columns: _*)
      } else {
        throw new IllegalArgumentException(s"Unsupported database: $jdbcPlatform.")
      }

    val configuration = new Configuration
    configuration.load(configUrl)

    var experiment: Experiment = null
    queryName match {
      case "Q1" =>
        val query = new Query1(plugins: _*)
        experiment = Parameters.createExperiment(experimentArg, query)
        experiment.getSubject.addConfiguration("plugins", args(1))
        experiment.getSubject.addConfiguration("query", args(3))
        val result = query(configuration, jdbcPlatform, createTableSource)(experiment)
        StdOut.printLimited(result, 10)
      case "Q3File" =>
        val query = new Query3File(plugins: _*)
        experiment = Parameters.createExperiment(experimentArg, query)
        experiment.getSubject.addConfiguration("plugins", args(1))
        experiment.getSubject.addConfiguration("query", args(3))
        val result = query(configuration)(experiment)
        StdOut.printLimited(result, 10)
      case "Q3" =>
        val query = new Query3Database(plugins: _*)
        experiment = Parameters.createExperiment(experimentArg, query)
        experiment.getSubject.addConfiguration("plugins", args(1))
        experiment.getSubject.addConfiguration("query", args(3))
        val result = query(configuration, jdbcPlatform, createTableSource)(experiment)
        StdOut.printLimited(result, 10)
      case "Q3Hybrid" =>
        val query = new Query3Hybrid(plugins: _*)
        experiment = Parameters.createExperiment(experimentArg, query)
        experiment.getSubject.addConfiguration("plugins", args(1))
        experiment.getSubject.addConfiguration("query", args(3))
        val result = query(configuration, jdbcPlatform, createTableSource)(experiment)
        StdOut.printLimited(result, 10)
      case other: String => {
        println(s"Unknown query: $other")
        sys.exit(1)
      }
    }

    // Store experiment data.
    ProfileDBHelper.store(experiment, configuration)
  }

}
