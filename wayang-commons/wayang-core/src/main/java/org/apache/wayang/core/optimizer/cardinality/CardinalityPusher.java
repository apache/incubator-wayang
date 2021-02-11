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
import org.apache.wayang.core.optimizer.costs.TimeEstimate;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.util.WayangArrays;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Objects;

/**
 * Pushes a input {@link CardinalityEstimate}s through an {@link Operator} and yields its output
 * {@link CardinalityEstimate}s. As an important side-effect, {@link Operator}s will store their {@link CardinalityEstimate}
 */
public abstract class CardinalityPusher {

    protected final Logger logger = LogManager.getLogger(this.getClass());

    protected final int[] relevantInputIndices;

    protected final int[] relevantOutputIndices;

    protected CardinalityPusher(Operator operator) {
        this(WayangArrays.range(operator.getNumInputs()), WayangArrays.range(operator.getNumOutputs()));
    }

    protected CardinalityPusher(int[] relevantInputIndices, int[] relevantOutputIndices) {
        this.relevantInputIndices = relevantInputIndices;
        this.relevantOutputIndices = relevantOutputIndices;
    }


    /**
     * Push the input {@link CardinalityEstimate}s of the {@code operatorContext} to the output {@link CardinalityEstimate}s.
     * If that leaded to an update, also update the {@link TimeEstimate}.
     *
     * @param opCtx         will be subject to the push
     * @param configuration potentially provides some estimation helpers
     * @return whether an update of the {@link CardinalityEstimate}s took place
     */
    public boolean push(OptimizationContext.OperatorContext opCtx, Configuration configuration) {
        assert opCtx != null;
        this.logger.trace("Pushing through {}.", opCtx.getOperator());

        assert Arrays.stream(this.relevantInputIndices).mapToObj(opCtx::getInputCardinality).noneMatch(Objects::isNull)
                : String.format("Incomplete input cardinalities for %s.", opCtx.getOperator());

        if (!this.canUpdate(opCtx)) {
            return false;
        }

        if (this.logger.isTraceEnabled()) {
            this.logger.trace("Pushing {} into {}.", Arrays.toString(opCtx.getInputCardinalities()), opCtx.getOperator());
        }
        this.doPush(opCtx, configuration);

        opCtx.updateCostEstimate();

        return true;
    }

    /**
     * @return whether a {@link #doPush(OptimizationContext.OperatorContext, Configuration)} execution might result in an update
     * of {@link CardinalityEstimate}s
     */
    protected boolean canUpdate(OptimizationContext.OperatorContext opCtx) {
        // We can update if..

        boolean hasUnmarkedOutputEstimates = false;
        for (int outputIndex : this.relevantOutputIndices) {
            // ...there are missing output estimates.
            if (opCtx.getOutputCardinality(outputIndex) == null) return true;

            // ...or if there are unmarked output estimates...
            if (hasUnmarkedOutputEstimates = !opCtx.isOutputMarked(outputIndex)) break;
        }

        // ...and marked input estimates.
        if (!hasUnmarkedOutputEstimates) return false;
        for (int inputIndex : this.relevantInputIndices) {
            if (opCtx.isInputMarked(inputIndex)) return true;
        }

        return false;
    }

    /**
     * Perform the actual push.
     */
    protected abstract void doPush(OptimizationContext.OperatorContext opCtx, Configuration configuration);

}
