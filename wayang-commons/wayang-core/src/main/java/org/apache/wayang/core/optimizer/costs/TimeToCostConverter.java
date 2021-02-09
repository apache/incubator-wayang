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

package org.apache.wayang.core.optimizer.costs;

import org.apache.wayang.core.optimizer.ProbabilisticDoubleInterval;

/**
 * This (linear) converter turns {@link TimeEstimate}s into cost estimates.
 */
public class TimeToCostConverter {

    /**
     * The fix costs to be paid in some context.
     */
    private final double fixCosts;

    /**
     * The costs per millisecond.
     */
    private final double costsPerMilli;

    /**
     * Creates a new instance
     *
     * @param fixCosts      the fix costs to be paid in some context.
     * @param costsPerMilli the costs per millisecond
     */
    public TimeToCostConverter(double fixCosts, double costsPerMilli) {
        this.fixCosts = fixCosts;
        this.costsPerMilli = costsPerMilli;
    }

    /**
     * Convert the given {@link TimeEstimate} into a cost estimate.
     *
     * @param timeEstimate the {@link TimeEstimate}
     * @return the cost estimate
     */
    public ProbabilisticDoubleInterval convert(TimeEstimate timeEstimate) {
        double lowerBound = this.fixCosts + this.costsPerMilli * timeEstimate.getLowerEstimate();
        double upperBound = this.fixCosts + this.costsPerMilli * timeEstimate.getUpperEstimate();
        return new ProbabilisticDoubleInterval(lowerBound, upperBound, timeEstimate.getCorrectnessProbability());
    }


    /**
     * Convert the given {@link TimeEstimate} into a cost estimate without considering the fix costs.
     *
     * @param timeEstimate the {@link TimeEstimate}
     * @return the cost estimate
     */
    public ProbabilisticDoubleInterval convertWithoutFixCosts(TimeEstimate timeEstimate) {
        double lowerBound = this.costsPerMilli * timeEstimate.getLowerEstimate();
        double upperBound = this.costsPerMilli * timeEstimate.getUpperEstimate();
        return new ProbabilisticDoubleInterval(lowerBound, upperBound, timeEstimate.getCorrectnessProbability());
    }

    /**
     * Get the fix costs, i.e., the overhead, that incur according to this instance.
     *
     * @return the fix costs
     */
    public double getFixCosts() {
        return this.fixCosts;
    }

    /**
     * Get the costs that incur per millisecond according to this instance.
     *
     * @return the costs per milliseond
     */
    public double getCostsPerMillisecond() {
        return this.costsPerMilli;
    }
}
