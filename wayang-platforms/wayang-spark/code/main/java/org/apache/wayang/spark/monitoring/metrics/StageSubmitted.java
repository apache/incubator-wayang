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

 * The StageSubmitted class implements the Stage and SerializableObject interfaces to represent a submitted stage in a distributed system.
 * It contains information about the stage's ID, number of tasks, name, status, details, executor ID, list of tasks, stage attempt ID, stage submission time,
 * and task metric. It also allows the setting and getting of each of these properties through various interface methods.
 */
public class StageSubmitted implements Stage, SerializableObject {
    private int id;
    private int tasks;
    private String stageName;
    private String status;
    private String details;
    private String executorID;
    private List<Task> listOfTasks;
    private int stageAttemptID;
    private long stageSubmissionTime;
    public TaskMetric getTaskMetric() {
        return taskMetric;
    }

    public void setTaskMetric(TaskMetric taskMetric) {
        this.taskMetric = taskMetric;
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
        this.stageAttemptID=id;
    }

    @Override
    public int getStageAttemptId() {
        return id;
    }

    @Override
    public void setListOfTasks(List<Task> tasks) {
        this.listOfTasks= tasks;
    }

    @Override
    public List<Task> getListOfTasks() {
        return listOfTasks;
    }

    private TaskMetric taskMetric;
    private String eventName;

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
        this.stageSubmissionTime=time;
    }

    @Override
    public long getSubmissionTime() {
        return this.stageSubmissionTime;
    }

    @Override
    public void setCompletionTime(long time) {

    }

    @Override
    public long getCompletionTime() {
        return 0;
    }
}
