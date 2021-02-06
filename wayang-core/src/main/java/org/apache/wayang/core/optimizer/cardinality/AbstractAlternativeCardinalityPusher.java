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
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.OperatorAlternative;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;

/**
 * Abstract {@link CardinalityPusher} implementation for {@link OperatorAlternative}s and subclasses.
 */
public abstract class AbstractAlternativeCardinalityPusher extends CardinalityPusher {

    /**
     * @see CardinalityPusher#CardinalityPusher(Operator)
     */
    public AbstractAlternativeCardinalityPusher(Operator operator) {
        super(operator);
    }

    /**
     * @see CardinalityPusher#CardinalityPusher(int[], int[])
     */
    public AbstractAlternativeCardinalityPusher(int[] relevantInputIndices, int[] relevantOutputIndices) {
        super(relevantInputIndices, relevantOutputIndices);
    }

    @Override
    protected boolean canUpdate(OptimizationContext.OperatorContext operatorContext) {
        // We always try to update because there might be internal updates.
        return true;
    }

    @Override
    protected void doPush(OptimizationContext.OperatorContext opCtx, Configuration configuration) {
        // Trigger the push for each of the alternativeTraversals.
        this.pushThroughAlternatives(opCtx, configuration);

        // Somehow merge the CardinalityEstimates from the alternativeTraversals to the final ones for the opCtx.
        this.pickCardinalities(opCtx);
    }

    /**
     * Do the pushing through all of the alternatives.
     *
     * @param opCtx         as in {@link #doPush(OptimizationContext.OperatorContext, Configuration)}
     * @param configuration as in {@link #doPush(OptimizationContext.OperatorContext, Configuration)}
     */
    public abstract void pushThroughAlternatives(OptimizationContext.OperatorContext opCtx, Configuration configuration);


    /**
     * Pick {@link CardinalityEstimate}s for each of the {@link OutputSlot}s within the {@code opCtx}.
     */
    protected void pickCardinalities(OptimizationContext.OperatorContext opCtx) {
        final OperatorAlternative operatorAlternative = (OperatorAlternative) opCtx.getOperator();

        // For each relevant OutputSlot of the OperatorAlternative...
        for (int outputIndex : this.relevantOutputIndices) {
            final OutputSlot<?> output = operatorAlternative.getOutput(outputIndex);
            CardinalityEstimate bestEstimate = opCtx.getOutputCardinality(output.getIndex());

            // ...and dor each corresponding OutputSlot in each Alternative...
            for (OperatorAlternative.Alternative alternative : operatorAlternative.getAlternatives()) {

                // ...pick the best CardinalityEstimate.
                final OutputSlot<?> innerOutput = alternative.getSlotMapping().resolveUpstream(output);
                if (innerOutput == null) continue;
                final OptimizationContext.OperatorContext innerOpCtx = opCtx.getOptimizationContext()
                        .getOperatorContext(innerOutput.getOwner());
                final CardinalityEstimate newEstimate = innerOpCtx.getOutputCardinality(innerOutput.getIndex());
                if (newEstimate == null) {
                    logger.warn("No cardinality estimate for {}.", innerOutput);
                    continue;
                }
                bestEstimate = this.choose(bestEstimate, newEstimate);
            }

            // Finalize the decision.
            opCtx.setOutputCardinality(output.getIndex(), bestEstimate);
        }
    }

    /**
     * Merge two {@link CardinalityEstimate} vectors (point-wise).
     */
    protected CardinalityEstimate choose(CardinalityEstimate estimate1, CardinalityEstimate estimate2) {
        // Make sure there are no nulls.
        if (estimate1 == null) {
            return estimate2;
        } else if (estimate2 == null) {
            return estimate1;
        }

        // Check for overrides.
        if (estimate1.isOverride()) return estimate1;
        if (estimate2.isOverride()) return estimate2;

        // Actually compare the two estimates.
        // TODO: Check if this is a good way. There are other palpable approaches (e.g., weighted average).
        if (estimate2.getCorrectnessProbability() > estimate1.getCorrectnessProbability()) {
            return estimate2;
        }
        return estimate1;
    }

}
