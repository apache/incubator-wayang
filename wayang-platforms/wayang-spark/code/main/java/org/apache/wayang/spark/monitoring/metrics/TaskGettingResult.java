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
import org.apache.wayang.spark.monitoring.interfaces.Task;
/**
 * Represents the getting results of a task execution.
 *
 * <p>Implementing the {@link Task} interface, this class provides methods to set and get the
 * attributes of a task, such as its ID, host IP, launch and finish times, and status. It also
 * implements the {@link SerializableObject} interface to allow serialization of the class.
 *
 * <p>The class also includes a {@link TaskMetric} object to store metrics related to the task,
 * and a {@link TaskStatusForRunning} object to provide information about the task's status during
 * execution.
 *
 * @author [Adeel Aslam]
 */
public class TaskGettingResult implements Task, SerializableObject {
    private String id;
    private String hostIP;
    private long taskId;
    private String executorID;
    private String taskStatus;
    private int Index;
    private int partition;
    private long launchTime;
    private long durationTime;
    private long finishTime;
    private long gettingResultTime;
    private boolean status;
    private int stageID;

    private String eventName;

    TaskStatusForRunning taskStatusForRunning=null;

    @Override
    public void setEventame(String name) {
        this.eventName=name;
    }

    @Override
    public String getEventName() {
        return eventName;
    }


    @Override
    public void setID(String id) {
        this.id=id;
    }

    @Override
    public String getID() {
        return id;
    }
    @Override
    public void setHostIP(String Ip) {
        this.hostIP=Ip;
    }

    @Override
    public String getHostIP() {
        return hostIP;
    }

    @Override
    public void setTaskID(long taskId) {
        this.taskId=taskId;
    }

    @Override
    public void setStageID(int id) {
        this.stageID=id;
    }

    @Override
    public int getStageID() {
        return stageID;
    }

    @Override
    public long getTaskID() {
        return taskId;
    }

    @Override
    public void setStringExecutorID(String executorID) {
        this.executorID=executorID;
    }

    @Override
    public String getExecutorID() {
        return executorID;
    }

    @Override
    public void setTaskStatus(String status) {
        this.taskStatus=status;
    }

    @Override
    public String getTaskStatus() {
        return taskStatus;
    }

    @Override
    public void setIndex(int index) {
        this.Index= index;

    }

    @Override
    public int getIndex() {
        return Index;
    }

    @Override
    public void setPartition(int partition) {
        this.partition=partition;
    }

    @Override
    public int getPartition() {
        return partition;
    }

    @Override
    public void setLaunchTime(long time) {
        this.launchTime=time;
    }

    @Override
    public long getLaunchTime() {
        return launchTime;
    }

    @Override
    public void setFinishTime(long time) {
        this.finishTime=time;
    }

    @Override
    public long getFinishTime() {
        return finishTime;
    }

    @Override
    public void setGettingTime(long time) {
        this.gettingResultTime=time;
    }

    @Override
    public long getGettingTime() {
        return gettingResultTime;
    }

    @Override
    public void setDurationTime(long time) {
        this.durationTime=time;
    }

    @Override
    public long getDurationTime() {
        return durationTime;
    }


    @Override
    public void setTaskStatus(boolean status) {
        this.status=status;
    }

    @Override
    public boolean getTaskSatus() {
        return status;
    }

    @Override
    public void setTaskStatusForRunning(TaskStatusForRunning taskStatusForRunning) {
        this.taskStatusForRunning=taskStatusForRunning;
    }

    @Override
    public TaskStatusForRunning getTaskStatusForRunning() {
        return taskStatusForRunning;
    }

    @Override
    public TaskMetric getTaskMetric() {
        return null;
    }

    @Override
    public void setTaskMetric(TaskMetric taskMetric) {

    }


}
