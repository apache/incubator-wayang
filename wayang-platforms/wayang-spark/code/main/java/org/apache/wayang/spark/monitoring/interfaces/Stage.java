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

import org.apache.wayang.spark.monitoring.metrics.TaskMetric;

import java.io.Serializable;
import java.util.List;
/**
 * This interface represents a stage in a data processing pipeline.
 * A stage is a set of tasks that are executed together as part of a larger computation.
 */
public interface Stage extends Serializable {

    /**
     * Sets the name of the event associated with this stage.
     * @param name the name of the event
     */
    void setEventame(String name);

    /**
     * Gets the name of the event associated with this stage.
     * @return the name of the event
     */
    String getEventName();

    /**
     * Sets the ID of this stage.
     * @param ID the ID of the stage
     */
    void setID(int ID);

    /**
     * Gets the ID of this stage.
     * @return the ID of the stage
     */
    int getID();

    /**
     * Sets the number of tasks associated with this stage.
     * @param tasks the number of tasks
     */
    void setNumberOfTasks(int tasks);

    /**
     * Gets the number of tasks associated with this stage.
     * @return the number of tasks
     */
    int getNumberOfTasks();

    /**
     * Sets the name of this stage.
     * @param name the name of the stage
     */
    void setStageName(String name);

    /**
     * Gets the name of this stage.
     * @return the name of the stage
     */
    String getStageName();

    /**
     * Sets the status of this stage.
     * @param status the status of the stage
     */
    void setStatus(String status);

    /**
     * Gets the status of this stage.
     * @return the status of the stage
     */
    String getStatus();

    /**
     * Sets the details of this stage.
     * @param details the details of the stage
     */
    void setDetails(String details);

    /**
     * Gets the details of this stage.
     * @return the details of the stage
     */
    String getDetails();

    /**
     * Sets the submission time of this stage.
     * @param time the submission time
     */
    void setSubmissionTime(long time);

    /**
     * Gets the submission time of this stage.
     * @return the submission time
     */
    long getSubmissionTime();

    /**
     * Sets the completion time of this stage.
     * @param time the completion time
     */
    void setCompletionTime(long time);

    /**
     * Gets the completion time of this stage.
     * @return the completion time
     */
    long getCompletionTime();

    /**
     * Gets the task metric associated with this stage.
     * @return the task metric
     */
    TaskMetric getTaskMetric();

    /**
     * Sets the task metric associated with this stage.
     * @param taskMetric the task metric
     */
    void setTaskMetric(TaskMetric taskMetric);

    /**
     * Sets the ID of the executor associated with this stage.
     * @param ID the executor ID
     */
    void setExecutorID(String ID);

    /**
     * Gets the ID of the executor associated with this stage.
     * @return the executor ID
     */
    String getExecutorID();

    /**
     * Sets the ID of the stage attempt.
     * @param id the stage attempt ID
     */
    void setStageAttemptId(int id);

    /**
     * Gets the ID of the stage attempt.
     * @return the stage attempt ID
     */
    int getStageAttemptId();
    /**
     * Sets the list of tasks to be performed.
     *
     * @param tasks a List of Task objects representing the tasks to be performed
     */
    void setListOfTasks(List<Task> tasks);
    /**
     * Retrieves the list of tasks to be performed.
     *
     * @return a List of Task objects representing the tasks to be performed
     */
    List<Task> getListOfTasks();



}
