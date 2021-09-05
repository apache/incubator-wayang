package org.qcri.rheem.apps.util

import de.hpi.isg.profiledb.store.model.{Experiment, Subject}

/**
  * Implementing objects can be the [[Subject]] of a [[Experiment]].
  */
trait ExperimentDescriptor {

  /**
    * Retrieves the name of the experiment subject.
    *
    * @return the name
    */
  def name: String = this.getClass.getSimpleName.replace("$", "")

  /**
    * Retrieves the version of the experiment subject.
    *
    * @return the version
    */
  def version: String

  /**
    * The described [[Subject]].
    */
  def createSubject = new Subject(this.name, this.version)

  /**
    * Create a new [[Experiment]] for the described [[Subject]].
    *
    * @param id   the ID for the [[Experiment]]
    * @param tags tags for the [[Experiment]]
    * @return the [[Experiment]]
    */
  def createExperiment(id: String, tags: String*) = new Experiment(id, this.createSubject, tags: _*)

}
