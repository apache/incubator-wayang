package org.qcri.rheem.apps.util

import java.io.File

import de.hpi.isg.profiledb.ProfileDB
import de.hpi.isg.profiledb.store.model.Experiment
import org.qcri.rheem.core.api.Configuration
import org.qcri.rheem.core.profiling.ProfileDBs

/**
  * Helper utility to employ with [[ProfileDB]].
  */
object ProfileDBHelper {

  /**
    * Stores the given [[Experiment]] if the [[Configuration]] defines a ProfileDB.
    *
    * @param experiment    the [[Experiment]]
    * @param configuration [[Configuration]]
    */
  def store(experiment: Experiment, configuration: Configuration) = {
    configuration.getStringProperty("rheem.apps.profiledb", null) match {
      case path: String => {
        println(s"Storing experiment '${experiment.getId}' to $path.")
        val profileDB = ProfileDBs.createProfileDB
        profileDB.append(new File(path), experiment)
      }
      case _ =>
    }
  }

}
