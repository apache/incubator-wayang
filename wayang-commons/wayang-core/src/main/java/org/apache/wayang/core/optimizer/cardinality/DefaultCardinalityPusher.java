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

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.configuration.KeyValueProvider;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;

/**
 * Default {@link CardinalityPusher} implementation. Bundles all {@link CardinalityEstimator}s of an {@link Operator}.
 */
public class DefaultCardinalityPusher extends CardinalityPusher {

    private final CardinalityEstimator[] cardinalityEstimators;

    public DefaultCardinalityPusher(Operator operator,
                                    KeyValueProvider<OutputSlot<?>, CardinalityEstimator> estimationProvider) {
        super(operator);
        this.cardinalityEstimators = this.initializeCardinalityEstimators(operator, estimationProvider);
    }

    public DefaultCardinalityPusher(Operator operator,
                                    int[] relevantInputIndices,
                                    int[] relevantOutputIndices,
                                    KeyValueProvider<OutputSlot<?>, CardinalityEstimator> estimationProvider) {
        super(relevantInputIndices, relevantOutputIndices);
        this.cardinalityEstimators = this.initializeCardinalityEstimators(operator, estimationProvider);
    }

    /**
     * Initializes the {@link CardinalityEstimator}s required by this instance.
     */
    private CardinalityEstimator[] initializeCardinalityEstimators(
            Operator operator,
            KeyValueProvider<OutputSlot<?>, CardinalityEstimator> estimationProvider) {

        final CardinalityEstimator[] cardinalityEstimators = new CardinalityEstimator[operator.getNumOutputs()];
        for (int outputIndex : this.relevantOutputIndices) {
            final CardinalityEstimator estimator = estimationProvider.provideFor(operator.getOutput(outputIndex));
            cardinalityEstimators[outputIndex] = estimator;
        }
        return cardinalityEstimators;
    }

    @Override
    protected void doPush(OptimizationContext.OperatorContext opCtx, Configuration configuration) {
        for (int outputIndex : this.relevantOutputIndices) {
            final CardinalityEstimator estimator = this.cardinalityEstimators[outputIndex];
            if (estimator != null) {
                opCtx.setOutputCardinality(
                        outputIndex,
                        estimator.estimate(opCtx.getOptimizationContext(), opCtx.getInputCardinalities())
                );
            }
        }
    }
}
