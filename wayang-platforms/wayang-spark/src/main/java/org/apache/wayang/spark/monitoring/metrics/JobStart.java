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

import org.apache.wayang.spark.monitoring.interfaces.Job;
import org.apache.wayang.spark.monitoring.interfaces.SerializableObject;
import org.apache.wayang.spark.monitoring.interfaces.Stage;
import scala.collection.Seq;

import java.util.List;
/**
 * JobStart class represents a job's start in a system.
 * It implements the Job and SerializableObject interfaces.
 */
public class JobStart implements Job, SerializableObject {
    private int id;
    private int productArity;
    private Seq<Object> seqStageId;
    private String eventName;
    private List<Stage> listOfStages;

    @Override
    public void setEventame(String name) {
        this.eventName=name;
    }

    @Override
    public String getEventName() {
        return eventName;
    }


    @Override
    public void setJobID(int jobID) {
        this.id=jobID;
    }

    @Override
    public int getJobID() {
        return id;
    }

    @Override
    public void setProductArity(int productArity) {
        this.productArity=productArity;
    }

    @Override
    public int getProductArity() {
        return productArity;
    }

    @Override
    public void setStageID(Seq<Object> stageId) {
        this.seqStageId=stageId;
    }

    @Override
    public Seq<Object> getStageID() {
        return seqStageId;
    }

    @Override
    public void setListOfStages(List<Stage> listOfStages) {
        this.listOfStages=listOfStages;
    }

    @Override
    public List<Stage> getListOfStages() {
        return listOfStages;
    }
}
