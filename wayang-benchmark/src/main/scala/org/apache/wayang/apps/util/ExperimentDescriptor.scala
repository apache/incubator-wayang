/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.apps.util

import org.apache.wayang.commons.util.profiledb.model.{Experiment, Subject}


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
