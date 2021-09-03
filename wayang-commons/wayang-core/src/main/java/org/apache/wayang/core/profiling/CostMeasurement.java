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

package org.apache.wayang.core.profiling;


import org.apache.wayang.commons.util.profiledb.model.Measurement;
import org.apache.wayang.commons.util.profiledb.model.Type;

/**
 * This measurement captures execution costs w.r.t. to Wayang's cost model.
 */
@Type("cost")
public class CostMeasurement extends Measurement {

    /**
     * The cost is generally captures as bounding box and this is the lower or upper value, respectively.
     */
    private double lowerCost, upperCost;

    /**
     * The probability of the cost measurement being correct.
     */
    private double probability;

    /**
     * Creates a new instance.
     *
     * @param id          the ID of the instance
     * @param lowerCost   the lower bound of the cost
     * @param upperCost   the upper bound of the cost
     * @param probability the probability of the actual cost being within the bounds
     */
    public CostMeasurement(String id, double lowerCost, double upperCost, double probability) {
        super(id);
        this.lowerCost = lowerCost;
        this.upperCost = upperCost;
        this.probability = probability;
    }

    /**
     * Deserialization constructor.
     */
    protected CostMeasurement() {
    }

    public double getLowerCost() {
        return this.lowerCost;
    }

    public void setLowerCost(double lowerCost) {
        this.lowerCost = lowerCost;
    }

    public double getUpperCost() {
        return this.upperCost;
    }

    public void setUpperCost(double upperCost) {
        this.upperCost = upperCost;
    }

    public double getProbability() {
        return this.probability;
    }

    public void setProbability(double probability) {
        this.probability = probability;
    }
}
