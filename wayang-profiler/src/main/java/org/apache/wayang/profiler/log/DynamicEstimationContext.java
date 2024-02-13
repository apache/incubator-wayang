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

package org.apache.wayang.profiler.log;

import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;
import org.apache.wayang.core.optimizer.costs.EstimationContext;

import java.util.Collection;

/**
 * {@link EstimationContext} implementation for {@link DynamicLoadEstimator}s.
 */
public class DynamicEstimationContext implements EstimationContext {

    private final EstimationContext wrappedEstimationContext;

    private final Individual individual;

    public DynamicEstimationContext(Individual individual, EstimationContext wrappedEstimationContext) {
        this.individual = individual;
        this.wrappedEstimationContext = wrappedEstimationContext;
    }

    @Override
    public CardinalityEstimate[] getInputCardinalities() {
        return this.wrappedEstimationContext.getInputCardinalities();
    }

    @Override
    public CardinalityEstimate[] getOutputCardinalities() {
        return this.wrappedEstimationContext.getOutputCardinalities();
    }

    @Override
    public double getDoubleProperty(String propertyKey, double fallback) {
        return this.wrappedEstimationContext.getDoubleProperty(propertyKey, fallback);
    }

    @Override
    public int getNumExecutions() {
        return this.wrappedEstimationContext.getNumExecutions();
    }

    @Override
    public Collection<String> getPropertyKeys() {
        return this.wrappedEstimationContext.getPropertyKeys();
    }

    public Individual getIndividual() {
        return this.individual;
    }
}
