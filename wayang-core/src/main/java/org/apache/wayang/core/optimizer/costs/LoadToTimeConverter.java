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

/**
 * Converts a {@link LoadEstimate} into a time estimate.
 */
public abstract class LoadToTimeConverter {

    public abstract TimeEstimate convert(LoadEstimate loadEstimate);

    /**
     * Create linear converter.
     *
     * @param millisPerUnit coefficient: number of milliseconds per resource usage unit
     * @return the new instance
     */
    public static LoadToTimeConverter createLinearCoverter(final double millisPerUnit) {
        return new LoadToTimeConverter() {
            @Override
            public TimeEstimate convert(LoadEstimate loadEstimate) {
                return new TimeEstimate(
                        (long) Math.ceil(millisPerUnit * loadEstimate.getLowerEstimate()),
                        (long) Math.ceil(millisPerUnit * loadEstimate.getUpperEstimate()),
                        loadEstimate.getCorrectnessProbability()
                );
            }

            @Override
            public String toString() {
                return String.format("LoadToTimeConverter[%,.2f units/ms]", 1d / millisPerUnit);
            }
        };
    }

}
