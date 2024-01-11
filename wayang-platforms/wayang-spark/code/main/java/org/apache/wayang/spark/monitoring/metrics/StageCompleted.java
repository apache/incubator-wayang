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

package org.apache.wayang.spark.monitoring.metrics;

import org.apache.wayang.spark.monitoring.interfaces.SerializableObject;
import org.apache.wayang.spark.monitoring.interfaces.Stage;
import org.apache.wayang.spark.monitoring.interfaces.Task;

import java.util.List;
/**
 * Represents a completed stage in a distributed computing system.
 *
 * This class implements the Stage interface and SerializableObject interface.
 *
 * The completed stage contains the following information:
 * - The ID of the stage
 * - The number of tasks in the stage
 * - The name of the stage
 * - The status of the stage
 * - The details of the stage
 * - The ID of the executor that executed the stage
 * - The stage attempt ID
 * - The task metric for the stage
 * - The list of tasks for the stage
 * - The event name for the stage
 * - The stage completion time
 *
 * This class provides methods to get and set the above information.
 */
public class StageCompleted implements Stage, SerializableObject {
    private int id;
    private int tasks;
    private String stageName;
    private String status;
    private String details;
    private String executorID;
    private int stateAttempt;
    private TaskMetric taskMetric;
    private List<Task> listOfTasks;
    private String eventName;
    private long stageCompletionTime;

    @Override
    public void setTaskMetric(TaskMetric taskMetric) {
        this.taskMetric=taskMetric;
    }


    @Override
    public void setExecutorID(String ID) {
        this.executorID=ID;
    }

    @Override
    public String getExecutorID() {
        return executorID;
    }

    @Override
    public void setStageAttemptId(int id) {
        this.id=id;
    }

    @Override
    public int getStageAttemptId() {
        return id;
    }

    @Override
    public void setListOfTasks(List<Task> tasks) {
        this.listOfTasks=tasks;
    }

    @Override
    public List<Task> getListOfTasks() {
        return listOfTasks;
    }


    @Override
    public void setEventame(String name) {
        this.eventName=name;
    }

    @Override
    public String getEventName() {
        return eventName;
    }


    @Override
    public void setID(int ID) {
        this.id=ID;
    }

    @Override
    public int getID() {
        return id;
    }

    @Override
    public void setNumberOfTasks(int tasks) {
        this.tasks=tasks;
    }

    @Override
    public int getNumberOfTasks() {
        return tasks;
    }

    @Override
    public void setStageName(String name) {
        this.stageName=name;
    }

    @Override
    public String getStageName() {
        return stageName;
    }

    @Override
    public void setStatus(String Status) {
        this.status=status;
    }

    @Override
    public String getStatus() {
        return status;
    }

    @Override
    public void setDetails(String details) {
        this.details=details;
    }

    @Override
    public String getDetails() {
        return details;
    }

    @Override
    public void setSubmissionTime(long time) {


    }

    @Override
    public long getSubmissionTime() {
        return 0;
    }

    @Override
    public void setCompletionTime(long time) {
            this.stageCompletionTime=time;
    }

    @Override
    public long getCompletionTime() {
        return stageCompletionTime;
    }

    @Override
    public TaskMetric getTaskMetric() {
        return taskMetric;
    }
}
