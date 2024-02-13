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

import java.io.Serializable;
/**
 * The Executor interface represents an executor in a Spark cluster. It defines methods for setting and getting various attributes of an executor.
 * These attributes include the name of the event, the ID of the stage the executor is running, the ID of the executor itself,
 * the attempt ID of the stage, the time at which the executor started, the host where the executor is running,
 * the total number of cores available to the executor, the resource information of the executor,
 * and the reason for which the executor was removed from the cluster.
 */

public interface Executor extends Serializable {
    /**
     * Sets the name of the event associated with this executor.
     * @param name The name of the event.
     */
    void setEventame(String name);

    /**
     * Returns the name of the event associated with this executor.
     * @return The name of the event.
     */
    String getEventName();

    /**
     * Sets the ID of the stage the executor is running.
     * @param id The ID of the stage.
     */
    void setStageID(int id);

    /**
     * Returns the ID of the stage the executor is running.
     * @return The ID of the stage.
     */
    int getStageID();

    /**
     * Sets the ID of this executor.
     * @param id The ID of the executor.
     */
    void setExecutorID(String id);

    /**
     * Returns the ID of this executor.
     * @return The ID of the executor.
     */
    String getExecutorID();

    /**
     * Sets the attempt ID of the stage.
     * @param id The attempt ID of the stage.
     */
    void stageAttempt(int id);

    /**
     * Returns the attempt ID of the stage.
     * @return The attempt ID of the stage.
     */
    int getStageAttempt();

    /**
     * Sets the time at which this executor started.
     * @param Time The start time of the executor.
     */
    void executorTime(long Time);

    /**
     * Returns the time at which this executor started.
     * @return The start time of the executor.
     */
    long getExecutorTime();

    /**
     * Sets the host where this executor is running.
     * @param host The host where the executor is running.
     */
    void setExecutorHost(String host);

    /**
     * Returns the host where this executor is running.
     * @return The host where the executor is running.
     */
    String getExecutorHost();

    /**
     * Sets the total number of cores available to this executor.
     * @param cores The total number of cores.
     */
    void setTotalCores(int cores);

    /**
     * Returns the total number of cores available to this executor.
     * @return The total number of cores.
     */
    int getTotalCores();

    /**
     * Sets the resource information of this executor.
     * @param resourceInfoId The resource information of the executor.
     */
    void setResourceInfo(int resourceInfoId);

    /**
     * Returns the resource information of this executor.
     * @return The resource information of the executor.
     */
    int getResourceInfo();

    /**
     * Sets the reason for which this executor was removed from the cluster.
     * @param reasonOfRemoval The reason for removal.
     */
    void setReasonOfRemoval(String reasonOfRemoval);

    /**
     * Returns the reason for which this executor was removed from the cluster.
     * @return The reason for removal.
     */
    String getReasonOfRemoval();


}
