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

package org.apache.wayang.core.optimizer.cardinality;

import org.apache.wayang.core.optimizer.ProbabilisticIntervalEstimate;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.util.Formats;
import org.apache.wayang.core.util.JsonSerializable;
import org.apache.wayang.core.util.json.WayangJsonObj;

/**
 * An estimate of cardinality within a {@link WayangPlan} expressed as a {@link ProbabilisticIntervalEstimate}.
 */
public class CardinalityEstimate extends ProbabilisticIntervalEstimate implements JsonSerializable {

    public static final CardinalityEstimate EMPTY_ESTIMATE = new CardinalityEstimate(0, 0, 1d);

    public CardinalityEstimate(long lowerEstimate, long upperEstimate, double correctnessProb) {
        super(lowerEstimate, upperEstimate, correctnessProb);
    }

    public CardinalityEstimate(long lowerEstimate, long upperEstimate, double correctnessProb, boolean isOverride) {
        super(lowerEstimate, upperEstimate, correctnessProb, isOverride);
    }

    public CardinalityEstimate plus(CardinalityEstimate that) {
        return new CardinalityEstimate(
                addSafe(this.getLowerEstimate(), that.getLowerEstimate()),
                addSafe(this.getUpperEstimate(), that.getUpperEstimate()),
                Math.min(this.getCorrectnessProbability(), that.getCorrectnessProbability())
        );
    }

    /**
     * Avoids buffer overflows while adding two positive {@code long}s.
     */
    private static final long addSafe(long a, long b) {
        assert a >= 0 && b >= 0;
        long sum = a + b;
        if (sum < a || sum < b) sum = Long.MAX_VALUE;
        return sum;
    }

    /**
     * Divides the estimate values, not the probability.
     *
     * @param denominator by which this instance should be divided
     * @return the quotient
     */
    public CardinalityEstimate divideBy(double denominator) {
        return new CardinalityEstimate(
                (long) Math.ceil(this.getLowerEstimate() / denominator),
                (long) Math.ceil(this.getUpperEstimate() / denominator),
                this.getCorrectnessProbability()
        );
    }

    /**
     * Parses the given {@link WayangJsonObj} to create a new instance.
     *
     * @param json that should be parsed
     * @return the new instance
     */
    public static CardinalityEstimate fromJson(WayangJsonObj json) {
        return new CardinalityEstimate(json.getLong("lowerBound"), json.getLong("upperBound"), json.getDouble("confidence"));
    }

    @Override
    public WayangJsonObj toJson() {
        return this.toJson(new WayangJsonObj());
    }

    /**
     * Serializes this instance to the given {@link WayangJsonObj}.
     *
     * @param json to which this instance should be serialized
     * @return {@code json}
     */
    public WayangJsonObj toJson(WayangJsonObj json) {
        json.put("lowerBound", this.getLowerEstimate());
        json.put("upperBound", this.getUpperEstimate());
        json.put("confidence", this.getCorrectnessProbability());
        return json;
    }

    @Override
    public String toString() {
        return String.format(
                "(%,d..%,d, %s)",
                this.getLowerEstimate(),
                this.getUpperEstimate(),
                Formats.formatPercentage(this.getCorrectnessProbability())
        );
    }
}
