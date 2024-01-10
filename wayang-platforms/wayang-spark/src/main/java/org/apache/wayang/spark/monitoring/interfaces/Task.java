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
/**
 * The Task interface represents a task in a distributed computing or data processing system.
 *
 * <p>This interface extends the SerializableObject interface and defines the methods that should
 * be implemented by a task in order to properly function in such a system.
 *
 * <p>The Task interface provides methods for setting and getting various properties of a task,
 * including its ID, status, and execution metrics.
 *
 * <p>The Task interface also includes an enum for representing the status of a task while it is running.
 *
 * @author [Adeel Aslam]
 */
public interface Task extends SerializableObject{
    /**
     * The TaskStatusForRunning enum represents the possible statuses of a task while it is running.
     *
     * <p>Each enum value corresponds to a specific status: FAILED, SUCCESS, KILLED, SUCCESSFUL,
     * RUNNING, FINISHED, or SPECULATIVE.
     *
     * <p>The FAILED status indicates that the task has failed and will not be able to complete
     * successfully. The SUCCESS status indicates that the task has completed successfully and
     * produced a result. The KILLED status indicates that the task was killed before it could
     * complete. The SUCCESSFUL status indicates that the task has completed successfully, but
     * did not produce a result. The RUNNING status indicates that the task is currently running.
     * The FINISHED status indicates that the task has finished running, but its result has not
     * yet been obtained. The SPECULATIVE status indicates that the task is running in a speculative
     * manner, in addition to the primary task.
     *
     * @author [Adeel Aslam]
     */
    public enum TaskStatusForRunning {
        FAILED, SUCCESS, KILLED, SUCCESSFUL,RUNNING,FINISHED, SPECULATIVE;
    }
    /**
     * Sets the name of the event associated with this task.
     *
     * @param name the name of the event
     */
    void setEventame(String name);
    /**
     * Gets the name of the event associated with this task.
     *
     * @return the name of the event
     */
    String getEventName();
    /**
     * Sets the ID of this task.
     *
     * @param id the task ID
     */
    void setID(String id);
    /**
     * Gets the ID of this task.
     *
     * @return the task ID
     */
    String getID();
    /**
     * Sets the IP address of the host machine executing this task.
     *
     * @param Ip the IP address of the host machine
     */
    void setHostIP(String Ip);
    /**
     * Gets the IP address of the host machine executing this task.
     *
     * @return the IP address of the host machine
     */
    String getHostIP();
    /**
     * Sets the ID of this task.
     *
     * @param taskId the ID of this task
     */
    void setTaskID(long taskId);
    /**
     * Sets the ID of the stage to which this task belongs.
     *
     * @param id the ID of the stage to which this task belongs
     */
    void setStageID(int id);
    /**
     * Returns the ID of the stage to which this task belongs.
     *
     * @return the ID of the stage to which this task belongs
     */
    int getStageID();
    /**
     * Returns the ID of this task.
     *
     * @return the ID of this task
     */
    long getTaskID();
    /**
     * Sets the ID of the executor assigned to this task.
     *
     * @param executorID the ID of the executor assigned to this task
     */
    void setStringExecutorID(String executorID);
    /**
     * Returns the ID of the executor assigned to this task.
     *
     * @return the ID of the executor assigned to this task
     */
    String getExecutorID();
    /**
     * Sets the status of this task.
     *
     * @param status the status of this task
     */
    void setTaskStatus(String status);
    /**
     * Returns the status of this task.
     *
     * @return the status of this task
     */
    String getTaskStatus();
    /**
     * Sets the index of this task.
     *
     * @param index the index of this task
     */
    void setIndex(int index);
    /**
     * Returns the index of this task.
     *
     * @return the index of this task
     */
    int getIndex();
    /**
     * Sets the partition of this task.
     *
     * @param partition the partition of this task
     */
    void setPartition(int partition);
    /**
     * Returns the partition of this task.
     *
     * @return the partition of this task
     */
    int getPartition();
    /**
     * Sets the launch time of this task.
     *
     * @param time the launch time of this task
     */
    void setLaunchTime(long time);
    /**
     * Returns the launch time of this task.
     *
     * @return the launch time of this task
     */
    long getLaunchTime();
    /**
     * Sets the finish time of this task.
     *
     * @param time the finish time of this task
     */
    void setFinishTime(long time);
    /**
     * Returns the finish time of this task.
     *
     * @return the finish time of this task
     */
    long getFinishTime();
    /**
     * Sets the getting time of this task.
     *
     * @param time the getting time of this task
     */
    void setGettingTime(long time);
    /**
     * Returns the getting time of this task.
     *
     * @return the getting time of this task
     */
    long getGettingTime();
    /**
     * Sets the duration time of this task.
     *
     * @param time the duration time of this task
     */
    void setDurationTime(long time);
    /**
     * Returns the duration time of this task.
     *
     * @return the duration time of this task
     */
    long getDurationTime();
    /**
     * Sets the status of this task.
     *
     * @param status the status of this task
     */
    void setTaskStatus(boolean status);
    /**
     * Returns the status of this task.
     *
     * @return the status of this task
     */
    boolean getTaskSatus();

    /**

     Sets the task status for running.
     @param taskStatusForRunning the {@link TaskStatusForRunning} status to be set for the task
     */
    void setTaskStatusForRunning(TaskStatusForRunning taskStatusForRunning);

    /**

     Returns the current task status for running.
     @return the {@link TaskStatusForRunning} status of the task
     */
    TaskStatusForRunning getTaskStatusForRunning();

    /**

     Returns the {@link TaskMetric} associated with this task.
     @return the {@link TaskMetric} of the task
     */
    org.apache.wayang.spark.monitoring.metrics.TaskMetric getTaskMetric();

    /**

     Sets the {@link TaskMetric} associated with this task.
     @param taskMetric the {@link TaskMetric} to be set for the task
     */
    void setTaskMetric(org.apache.wayang.spark.monitoring.metrics.TaskMetric taskMetric);
    // void setTaskRunningStatus(boolean status)


}
