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

import java.util.Collection;
import java.util.LinkedList;
import java.util.function.Function;

/**
 * {@link LoadProfileEstimator} that estimates a predefined {@link LoadProfile}.
 */
public class ConstantLoadProfileEstimator implements LoadProfileEstimator {

    /**
     * The constant {@link LoadProfile} to estimate.
     */
    private final LoadProfile loadProfile;

    /**
     * Nested {@link LoadProfileEstimator}s together with a {@link Function} to extract the estimated
     * {@code Artifact} from the {@code Artifact}s subject to this instance.
     */
    private Collection<LoadProfileEstimator> nestedEstimators = new LinkedList<>();

    public ConstantLoadProfileEstimator(LoadProfile loadProfile) {
        this.loadProfile = loadProfile;
    }

    @Override
    public void nest(LoadProfileEstimator nestedEstimator) {
        this.nestedEstimators.add(nestedEstimator);
    }

    @Override
    public LoadProfile estimate(EstimationContext context) {
        return this.loadProfile;
    }

    @Override
    public Collection<LoadProfileEstimator> getNestedEstimators() {
        return this.nestedEstimators;
    }

    @Override
    public String getConfigurationKey() {
        return null;
    }

    @Override
    public LoadProfileEstimator copy() {
        // No copying required.
        return this;
    }
}
