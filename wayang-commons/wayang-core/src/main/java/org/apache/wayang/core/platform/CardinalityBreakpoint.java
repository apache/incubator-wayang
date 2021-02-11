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

package org.apache.wayang.core.platform;

import org.apache.commons.lang3.Validate;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;
import org.apache.wayang.core.plan.executionplan.Channel;
import org.apache.wayang.core.plan.executionplan.ExecutionStage;
import org.apache.wayang.core.plan.wayangplan.InputSlot;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.core.plan.wayangplan.Slot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * {@link Breakpoint} implementation that is based on the {@link CardinalityEstimate}s of {@link Channel}s.
 * <p>Specifically, this implementation requires that <i>all</i> {@link CardinalityEstimate}s of the inbound
 * {@link Channel}s of an {@link ExecutionStage} are to a certain extent accurate within a given probability.</p>
 */
public class CardinalityBreakpoint implements Breakpoint {

    private final Logger logger = LogManager.getLogger(this.getClass());

    private final double spreadSmoothing;

    private final double minConfidence;

    private final double maxSpread;

    /**
     * Creates a new instance.
     *
     * @param configuration provides configuration properties
     */
    public CardinalityBreakpoint(Configuration configuration) {
        this(
                configuration.getDoubleProperty("wayang.core.optimizer.cardinality.minconfidence"),
                configuration.getDoubleProperty("wayang.core.optimizer.cardinality.maxspread"),
                configuration.getDoubleProperty("wayang.core.optimizer.cardinality.spreadsmoothing")
        );
    }

    /**
     * Creates a new instance.
     *
     * @param minConfidence the minimum confidence of the {@link CardinalityEstimate}s
     * @param maxSpread     the minimum accuracy of the {@link CardinalityEstimate}s
     */
    public CardinalityBreakpoint(double minConfidence, double maxSpread, double spreadSmoothing) {
        Validate.inclusiveBetween(0, 1, minConfidence);
        Validate.isTrue(maxSpread >= 1);
        this.minConfidence = minConfidence;
        this.maxSpread = maxSpread;
        this.spreadSmoothing = spreadSmoothing;
    }

    @Override
    public boolean permitsExecutionOf(ExecutionStage stage,
                                      ExecutionState state,
                                      OptimizationContext optimizationContext) {

        for (Channel channel : stage.getInboundChannels()) {
            final CardinalityEstimate cardinalityEstimate = this.getCardinalityEstimate(channel, optimizationContext);
            if (cardinalityEstimate == null) {
                // TODO: We might need to look inside of LoopContexts.
                this.logger.warn("Could not find a cardinality estimate for {}.", channel);
                // Be conservative here.
                return false;
            } else {
                if (!this.approves(cardinalityEstimate)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Retrieves a {@link CardinalityEstimate} for the given {@link Channel} by searching at suitable places in
     * the {@link OptimizationContext}.
     *
     * @param channel             whose {@link CardinalityEstimate} is requested
     * @param optimizationContext contains {@link CardinalityEstimate}s
     * @return any found {@link CardinalityEstimate} or {@code null} if none could be found
     */
    private CardinalityEstimate getCardinalityEstimate(Channel channel, OptimizationContext optimizationContext) {
        // Try to find a corresponding Slot for that we have a CardinalityEstimate.
        for (Slot<?> slot : channel.getCorrespondingSlots()) {
            final OptimizationContext.OperatorContext operatorContext = optimizationContext.getOperatorContext(slot.getOwner());
            if (operatorContext == null) {
                logger.warn("No estimates available for {}.", slot.getOwner());
                continue;
            }
            if (slot instanceof InputSlot) {
                return operatorContext.getInputCardinality(slot.getIndex());
            } else {
                assert slot instanceof OutputSlot;
                return operatorContext.getOutputCardinality(slot.getIndex());
            }
        }

        return null;
    }

    /**
     * Whether the given {@link CardinalityEstimate} does not require a breakpoint.
     *
     * @param cardinalityEstimate the {@link CardinalityEstimate}
     * @return whether no breakpoint is needed
     */
    public boolean approves(CardinalityEstimate cardinalityEstimate) {
        return cardinalityEstimate.getCorrectnessProbability() >= this.minConfidence
                && this.calculateSpread(cardinalityEstimate) <= this.maxSpread;
    }

    public double calculateSpread(CardinalityEstimate cardinalityEstimate) {
        return ((double) cardinalityEstimate.getUpperEstimate() + this.spreadSmoothing)
                / ((double) cardinalityEstimate.getLowerEstimate() + this.spreadSmoothing);
    }

}
