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
import org.apache.wayang.spark.monitoring.interfaces.Executor;
import org.apache.wayang.spark.monitoring.interfaces.SerializableObject;

/**
 * An event class representing the removal of an executor.
 * Implements the Executor interface and SerializableObject interface.
 */
public class ExecutorRemoved implements Executor, SerializableObject {
    private int stageId;
    private String executorID;
    private int stageAttempt;
    private long time;
    private String executorHost;
    private int totalCores;

    public String getReasonOfRemoval() {
        return reasonOfRemoval;
    }

    public void setReasonOfRemoval(String reasonOfRemoval) {
        this.reasonOfRemoval = reasonOfRemoval;
    }

    private String reasonOfRemoval;

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
    public void setStageID(int id) {
        this.stageId=id;
    }

    @Override
    public int getStageID() {
        return stageId;
    }

    @Override
    public void setExecutorID(String id) {
        this.executorID=id;
    }

    @Override
    public String getExecutorID() {
        return executorID;
    }

    @Override
    public void stageAttempt(int id) {
        this.stageAttempt=id;
    }

    @Override
    public int getStageAttempt() {
        return stageAttempt;
    }

    @Override
    public void executorTime(long Time) {
        this.time=time;
    }

    @Override
    public long getExecutorTime() {
        return time;
    }

    @Override
    public void setExecutorHost(String host) {
        this.executorHost=host;
    }

    @Override
    public String getExecutorHost() {
        return executorHost;
    }

    @Override
    public void setTotalCores(int cores) {
        this.totalCores=cores;
    }

    @Override
    public int getTotalCores() {
        return totalCores;
    }

    @Override
    public void setResourceInfo(int resourceInfoId) {

    }

    @Override
    public int getResourceInfo() {
        return 0;
    }
}
