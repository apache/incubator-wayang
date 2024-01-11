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
import java.util.List;
/**
 * This interface represents an application metrics in a Spark cluster.
 *  The implementing classes must be serializable.
 */
public interface Application extends Serializable {
    /**
     * Sets the name of the event associated with the application.
     *
     * @param name the name of the event
     */
    void setEventame(String name);
    /**
     * Returns the name of the event associated with the application.
     *
     * @return the name of the event
     */
    String getEventName();
    /**
     * Sets the name of the application.
     *
     * @param name the name of the application
     */
    void setName(String name);
    /**
     * Returns the name of the application.
     *
     * @return the name of the application
     */
    String getName();
    /**
     * Sets the start time of the application.
     *
     * @param time the start time of the application
     */
    void setStartTime(long time);
    /**
     * Returns the start time of the application.
     *
     * @return the start time of the application
     */
    long getTime();
    /**
     * Sets the ID of the application.
     *
     * @param id the ID of the application
     */
    void setAppID(String id);
    /**
     * Returns the ID of the application.
     *
     * @return the ID of the application
     */
    String getAppID();
    /**
     * Sets the user associated with the Spark application.
     *
     * @param user the user associated with the Spark application
     */
    void setSparkUser(String user);
    /**
     * Returns the user associated with the Spark application.
     *
     * @return the user associated with the Spark application
     */
    String getSparkUser();
    /**
     * Sets the list of jobs associated with the application.
     *
     * @param listOfJobs the list of jobs associated with the application
     */
    void setListOfJobs(List<Job> listOfJobs);
    /**
     * Returns the list of jobs associated with the application.
     *
     * @return the list of jobs associated with the application
     */
    List<Job> getListOfjobs();
}
