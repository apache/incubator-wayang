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
/**

 This class represents the metrics for a task in the Apache Wayang monitoring system.

 It implements the TaskMetric interface and SerializableObject interface to allow for serialization.
 */
public class TaskMetric  implements org.apache.wayang.spark.monitoring.interfaces.TaskMetric, SerializableObject {
    public long getExecutorCPUTime() {
        return executorCPUTime;
    }

    public void setExecutorCPUTime(long executorCPUTime) {
        this.executorCPUTime = executorCPUTime;
    }

    private long executorCPUTime;
    private long bytesRead;
    private long executorDeserializeCpuTime;
    private  long executorDeserializeTime;
    private long DiskByteSpilled;
    private long time;
    private long JVMGCTime;
    private long peakExecutionMemory;
    private long resultSize;
    private long resultSerializationTime;
    private long recordsWritten;
    private  long bytesWritten;
    @Override
    public void setBytesRead(long bytesRead) {
        this.bytesRead=bytesRead;
    }

    @Override
    public long getByteRead() {
        return bytesRead;
    }

    @Override
    public void setExecutorDeserializeCpuTime(long executorDeserializeCpuTime) {
        this.executorDeserializeCpuTime=executorDeserializeCpuTime;
    }

    @Override
    public long getExecutorDeserializeCpuTime() {
        return executorDeserializeCpuTime;
    }

    @Override
    public void setExecutorDeserializeTime(long executorDeserializeTime) {
        this.executorDeserializeTime=executorDeserializeTime;
    }

    @Override
    public long getExecutorDeserializeTime() {
        return executorDeserializeTime;
    }

    @Override
    public void setDiskBytesSpilled(long DiskByteSpilled) {
        this. DiskByteSpilled=DiskByteSpilled;
    }

    @Override
    public long getDiskBytesSpilled() {
        return DiskByteSpilled;
    }

    @Override
    public void setExecutorRunTime(long time) {
        this.time=time;
    }

    @Override
    public long getexecutorRunTime() {
        return time;
    }

    @Override
    public void setjvmGCTime(long time) {
        this.JVMGCTime=time;
    }

    @Override
    public long getJVMGCTime() {
        return JVMGCTime;
    }

    @Override
    public void setPeakExecutionMemory(long peakExecutionMemory) {
        this. peakExecutionMemory=peakExecutionMemory;
    }

    @Override
    public long getPeakExecutionMemory() {
        return peakExecutionMemory;
    }

    @Override
    public void setResultSize(long resultSize) {
        this. resultSize=resultSize;
    }

    @Override
    public long getResultSize() {
        return resultSize;
    }

    @Override
    public void setResultSerializationTime(long resultSerializationTime) {
        this. resultSerializationTime=resultSerializationTime;
    }

    @Override
    public long getResultSerializationTime() {
        return resultSerializationTime;
    }

    @Override
    public void setRecordsWritten(long recordsWritten) {
        this.recordsWritten= recordsWritten;
    }

    @Override
    public long getRecordsWrittern() {
        return recordsWritten;
    }

    @Override
    public void setBytesWritten(long bytesWritten) {
        this.bytesWritten=bytesWritten;
    }

    @Override
    public long getBytesWrittern() {
        return bytesWritten;
    }
}
