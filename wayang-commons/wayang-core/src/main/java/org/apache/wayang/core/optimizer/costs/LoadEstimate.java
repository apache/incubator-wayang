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

import org.apache.wayang.core.optimizer.ProbabilisticIntervalEstimate;
import org.apache.wayang.core.util.JsonSerializable;
import org.apache.wayang.core.util.JsonSerializables;
import org.apache.wayang.core.util.json.WayangJsonObj;

/**
 * An estimate of costs of some executable code expressed as a {@link ProbabilisticIntervalEstimate}.
 */
public class LoadEstimate extends ProbabilisticIntervalEstimate implements JsonSerializable {

    public LoadEstimate(long exactValue) {
        this(exactValue, exactValue, 1d);
    }

    public LoadEstimate(long lowerEstimate, long upperEstimate, double correctnessProb) {
        super(lowerEstimate, upperEstimate, correctnessProb);
    }

    /**
     * Multiplies the estimated load. The correctness probability is not altered.
     *
     * @param n scalar to multiply with
     * @return the product
     */
    public LoadEstimate times(int n) {
        return new LoadEstimate(this.getLowerEstimate() * n, this.getUpperEstimate() * n, this.getCorrectnessProbability());
    }

    /**
     * Adds a this and the given instance.
     *
     * @param that the other summand
     * @return a new instance representing the sum
     */
    public LoadEstimate plus(LoadEstimate that) {
        return new LoadEstimate(
                this.getLowerEstimate() + that.getLowerEstimate(),
                this.getUpperEstimate() + that.getUpperEstimate(),
                this.getCorrectnessProbability() * that.getCorrectnessProbability()
        );
    }

    /**
     * Adds a the and the given instances.
     *
     * @param estimate1 the one summand or {@code null}
     * @param estimate2 the other summand or {@code null}
     * @return a new instance representing the sum or {@code null} if both summands are {@code null}
     */
    public static LoadEstimate add(LoadEstimate estimate1, LoadEstimate estimate2) {
        return estimate1 == null ?
                estimate2 :
                (estimate2 == null ? estimate1 : estimate1.plus(estimate2));
    }

    @Override
    public WayangJsonObj toJson() {
        WayangJsonObj json = new WayangJsonObj();
        json.put("lower", JsonSerializables.serialize(this.getLowerEstimate(), false));
        json.put("upper", JsonSerializables.serialize(this.getUpperEstimate(), false));
        json.put("prob", JsonSerializables.serialize(this.getCorrectnessProbability(), false));
        return json;
    }

    @SuppressWarnings("unused")
    public static LoadEstimate fromJson(WayangJsonObj wayangJsonObj) {
        return new LoadEstimate(
                wayangJsonObj.getLong("lower"),
                wayangJsonObj.getLong("upper"),
                wayangJsonObj.getDouble("prob")
        );
    }
}
