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
 * The TaskMetric interface defines the methods used to set and retrieve
 * performance metrics for a given task.
 */
public interface TaskMetric extends Serializable {

    /**
     * Sets the number of bytes read by the task.
     *
     * @param bytesRead the number of bytes read
     */
    void setBytesRead(long bytesRead);

    /**
     * Gets the number of bytes read by the task.
     *
     * @return the number of bytes read
     */
    long getByteRead();

    /**
     * Sets the CPU time used for deserializing the task executor.
     *
     * @param executorDeserializeCpuTime the CPU time used for deserializing the executor
     */
    void setExecutorDeserializeCpuTime(long executorDeserializeCpuTime );

    /**
     * Gets the CPU time used for deserializing the task executor.
     *
     * @return the CPU time used for deserializing the executor
     */
    long getExecutorDeserializeCpuTime();

    /**
     * Sets the time taken to deserialize the task executor.
     *
     * @param executorDeserializeTime the time taken to deserialize the executor
     */
    void setExecutorDeserializeTime(long executorDeserializeTime);

    /**
     * Gets the time taken to deserialize the task executor.
     *
     * @return the time taken to deserialize the executor
     */
    long getExecutorDeserializeTime();

    /**
     * Sets the number of bytes spilled to disk by the task.
     *
     * @param DiskByteSpilled the number of bytes spilled to disk
     */
    void setDiskBytesSpilled(long DiskByteSpilled);

    /**
     * Gets the number of bytes spilled to disk by the task.
     *
     * @return the number of bytes spilled to disk
     */
    long getDiskBytesSpilled();

    /**
     * Sets the total time taken by the task executor to run.
     *
     * @param time the time taken by the executor to run
     */
    void setExecutorRunTime(long time);

    /**
     * Gets the total time taken by the task executor to run.
     *
     * @return the time taken by the executor to run
     */
    long getexecutorRunTime();

    /**
     * Sets the amount of time spent by the JVM on garbage collection.
     *
     * @param time the amount of time spent on garbage collection
     */
    void setjvmGCTime(long time);

    /**
     * Gets the amount of time spent by the JVM on garbage collection.
     *
     * @return the amount of time spent on garbage collection
     */
    long getJVMGCTime();

    /**
     * Sets the peak execution memory used by the task executor.
     *
     * @param peakExecutionMemory the peak execution memory used by the executor
     */
    void setPeakExecutionMemory(long peakExecutionMemory);

    /**
     * Gets the peak execution memory used by the task executor.
     *
     * @return the peak execution memory used by the executor
     */
    long getPeakExecutionMemory();

    /**
     * Sets the size of the result produced by the task.
     *
     * @param resultSize the size of the result produced
     */
    void setResultSize(long resultSize);

    /**
     * Gets the size of the result produced by the task.
     *
     * @return the size of the result produced
     */
    long getResultSize();
    /**
     * Sets the time taken to serialize the result of the task.
     *
     * @param resultSerializationTime the time taken to serialize the result
     */
    void  setResultSerializationTime(long resultSerializationTime);
    /**
     * Returns the time taken to serialize the result of the task.
     *
     * @return the time taken to serialize the result
     */
    long getResultSerializationTime();
    /**
     * Sets the number of records written by the task.
     *
     * @param recordsWritten the number of records written
     */
    void setRecordsWritten(long recordsWritten);
    /**
     * Returns the number of records written by the task.
     *
     * @return the number of records written
     */
    long getRecordsWrittern();
    /**
     * Sets the number of bytes written by the task.
     *
     * @param bytesWritten the number of bytes written
     */
    void setBytesWritten(long bytesWritten);
    /**
     * Returns the number of bytes written by the task.
     *
     * @return the number of bytes written
     */
    long getBytesWrittern ();
}
