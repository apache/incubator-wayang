
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

package org.apache.wayang.spark.monitoring.interfaces;

import scala.collection.Seq;

import java.io.Serializable;
import java.util.List;
/**
 * The Job interface represents a job to be executed in a distributed system.
 * A job comprises one or more stages, and contains metadata about the job
 * such as its ID, product arity, and event name.
 */
public interface Job extends Serializable {

    /**
     * Sets the name of the event associated with this job.
     *
     * @param name the name of the event
     */
    void setEventame(String name);

    /**
     * Returns the name of the event associated with this job.
     *
     * @return the name of the event
     */
    String getEventName();

    /**
     * Sets the unique identifier for this job.
     *
     * @param jobID the unique identifier for this job
     */
    void setJobID(int jobID);

    /**
     * Returns the unique identifier for this job.
     *
     * @return the unique identifier for this job
     */
    int getJobID();

    /**
     * Sets the number of output products produced by this job.
     *
     * @param productArity the number of output products produced by this job
     */
    void setProductArity(int productArity);

    /**
     * Returns the number of output products produced by this job.
     *
     * @return the number of output products produced by this job
     */
    int getProductArity();

    /**
     * Sets the stage ID associated with this job.
     *
     * @param stageId the stage ID associated with this job
     */
    void setStageID(Seq<Object> stageId);

    /**
     * Returns the stage ID associated with this job.
     *
     * @return the stage ID associated with this job
     */
    Seq<Object> getStageID();

    /**
     * Sets the list of stages comprising this job.
     *
     * @param listOfStages the list of stages comprising this job
     */
    void setListOfStages(List<Stage> listOfStages);

    /**
     * Returns the list of stages comprising this job.
     *
     * @return the list of stages comprising this job
     */
    List<Stage> getListOfStages();
}
