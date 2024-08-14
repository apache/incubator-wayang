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
import org.apache.wayang.core.optimizer.OptimizationUtils;
import org.apache.wayang.core.optimizer.enumeration.LoopImplementation;
import org.apache.wayang.core.optimizer.enumeration.PlanImplementation;
import org.apache.wayang.core.plan.wayangplan.LoopSubplan;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.ExecutionState;
import org.apache.wayang.core.platform.Junction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.Map;

/**
 * Handles the {@link CardinalityEstimate}s of a {@link WayangPlan}.
 */
public class CardinalityEstimatorManager {

    private final Logger logger = LogManager.getLogger(this.getClass());

    /**
     * The {@link WayangPlan} whose cardinalities are being managed.
     */
    private final WayangPlan wayangPlan;

    /**
     * Keeps the {@link CardinalityEstimate}s around.
     */
    private final OptimizationContext optimizationContext;

    /**
     * Provides {@link CardinalityEstimator}s etc.
     */
    private final Configuration configuration;

    private CardinalityEstimationTraversal planTraversal;

    public CardinalityEstimatorManager(WayangPlan wayangPlan,
                                       OptimizationContext optimizationContext,
                                       Configuration configuration) {
        this.wayangPlan = wayangPlan;
        this.optimizationContext = optimizationContext;
        this.configuration = configuration;
    }

    /**
     * Traverse the {@link WayangPlan}, thereby updating {@link CardinalityEstimate}s.
     *
     * @return whether any {@link CardinalityEstimate}s have been updated
     */
    public boolean pushCardinalities() {
        boolean isUpdated = this.getPlanTraversal().traverse(this.optimizationContext, this.configuration);
        this.optimizationContext.clearMarks();
        return isUpdated;
    }

    /**
     * Traverse the {@link WayangPlan}, thereby updating {@link CardinalityEstimate}s. Also update conversion
     * {@link Operator} cardinalities as provided by the {@link PlanImplementation}.
     *
     * @param planImplementation that has conversion {@link Operator}s
     * @return whether any {@link CardinalityEstimate}s have been updated
     */
    public boolean pushCardinalities(PlanImplementation planImplementation) {
        boolean isUpdated = this.getPlanTraversal().traverse(this.optimizationContext, this.configuration);
        planImplementation.getLoopImplementations().keySet().forEach(
                loop ->  this.optimizationContext.getNestedLoopContext(loop).getAggregateContext().updateOperatorContexts()
        );

        this.updateConversionOperatorCardinalities(planImplementation, this.optimizationContext, 0);
        this.optimizationContext.clearMarks();
        return isUpdated;
    }

    /**
     * Update conversion {@link Operator} cardinalities.
     *
     * @param planImplementation       that has conversion {@link Operator}s
     * @param optimizationContext      that provides optimization information for the {@code planImplementation}
     * @param optimizationContextIndex the index of the {@code optimizationContext} (important inside of loops)
     */
    public void updateConversionOperatorCardinalities(
            PlanImplementation planImplementation,
            OptimizationContext optimizationContext,
            int optimizationContextIndex) {

        // Update conversion operators outside of loops.
        for (OptimizationContext.OperatorContext operatorContext : optimizationContext.getLocalOperatorContexts().values()) {
            final Operator operator = operatorContext.getOperator();
            for (int outputIndex = 0; outputIndex < operator.getNumOutputs(); outputIndex++) {
                if (operatorContext.isOutputMarked(outputIndex)) {
                    final OutputSlot<?> output = operator.getOutput(outputIndex);
                    final Junction junction = planImplementation.getJunction(output);
                    if (junction == null) continue;

                    final CardinalityEstimate cardinality = operatorContext.getOutputCardinality(outputIndex);
                    OptimizationContext jctOptimizationContext = junction.getOptimizationContexts().get(0);
                    for (OptimizationContext.OperatorContext jctOpCtx : jctOptimizationContext.getLocalOperatorContexts().values()) {
                        final OptimizationContext.OperatorContext opCtx = optimizationContext.getOperatorContext(jctOpCtx.getOperator());
                        if (opCtx.getInputCardinalities().length == 1) opCtx.setInputCardinality(0, cardinality);
                        if (opCtx.getOutputCardinalities().length == 1) opCtx.setOutputCardinality(0, cardinality);
                        opCtx.updateCostEstimate();
                    }
                }
            }
        }
        // Visit loops.
        for (Map.Entry<LoopSubplan, LoopImplementation> entry : planImplementation.getLoopImplementations().entrySet()) {
            final LoopSubplan loop = entry.getKey();
            final LoopImplementation loopImplementation = entry.getValue();
            final OptimizationContext.LoopContext loopContext = optimizationContext.getNestedLoopContext(loop);
            assert loopContext != null;

            for (int iteration = 0; iteration < loopContext.getIterationContexts().size(); iteration++) {
                final OptimizationContext iterationContext = loopContext.getIterationContext(iteration);
                this.updateConversionOperatorCardinalities(
                        loopImplementation.getSingleIterationImplementation().getBodyImplementation(),
                        iterationContext,
                        iteration
                );
            }
        }
    }

    public CardinalityEstimationTraversal getPlanTraversal() {
        if (this.planTraversal == null) {
            this.planTraversal = CardinalityEstimationTraversal.createPushTraversal(
                    Collections.emptyList(),
                    this.wayangPlan.collectReachableTopLevelSources(),
                    this.configuration
            );
        }
        return this.planTraversal;
    }

    /**
     * Injects the cardinalities of a current {@link ExecutionState} into its associated {@link WayangPlan}
     * (or its {@link OptimizationContext}, respectively) and then reperforms the cardinality estimation.
     *
     * @return whether any cardinalities have been injected
     */
    public boolean pushCardinalityUpdates(ExecutionState executionState, PlanImplementation planImplementation) {
        boolean isInjected = this.injectMeasuredCardinalities(executionState);
        if (isInjected) this.pushCardinalities(planImplementation);
        return isInjected;
    }

    /**
     * Injects the cardinalities of a current {@link ExecutionState} into its associated {@link WayangPlan}.
     *
     * @return whether any cardinalities have been injected
     */
    private boolean injectMeasuredCardinalities(ExecutionState executionState) {
        executionState.getCardinalityMeasurements().forEach(this::injectMeasuredCardinality);
        return !executionState.getCardinalityMeasurements().isEmpty();
    }

    /**
     * Injects the measured cardinality of a {@link ChannelInstance}.
     */
    private void injectMeasuredCardinality(ChannelInstance channelInstance) {
        assert channelInstance.wasProduced();
        assert channelInstance.isMarkedForInstrumentation();

        // Obtain cardinality measurement.
        final long cardinality = channelInstance.getMeasuredCardinality().getAsLong();

        // Try to inject into the WayangPlan Operator output.
        final OutputSlot<?> wayangPlanOutput = OptimizationUtils.findWayangPlanOutputSlotFor(channelInstance.getChannel());
        int outputIndex = wayangPlanOutput == null ? 0 : wayangPlanOutput.getIndex();
        OptimizationContext optimizationContext = channelInstance.getProducerOperatorContext().getOptimizationContext();
        final OptimizationContext.OperatorContext wayangPlanOperatorCtx = optimizationContext.getOperatorContext(wayangPlanOutput.getOwner());
        if (wayangPlanOperatorCtx != null) {
            this.injectMeasuredCardinality(cardinality, wayangPlanOperatorCtx, outputIndex);
        } else {
            this.logger.warn("Could not inject cardinality measurement {} for {}.", cardinality, wayangPlanOutput);
        }
    }

    /**
     * Injects the measured {@code cardinality}.
     */
    private void injectMeasuredCardinality(long cardinality, OptimizationContext.OperatorContext targetOperatorContext, int outputIndex) {
        // Build the new CardinalityEstimate.
        final CardinalityEstimate newCardinality = new CardinalityEstimate(cardinality, cardinality, 1d, true);
        final CardinalityEstimate oldCardinality = targetOperatorContext.getOutputCardinality(outputIndex);
        if (!newCardinality.equals(oldCardinality)) {
            if (this.logger.isInfoEnabled()) {
                this.logger.info("Updating cardinality of {}'s output {} from {} to {}.",
                        targetOperatorContext.getOperator(),
                        outputIndex,
                        oldCardinality,
                        newCardinality
                );
            }
            targetOperatorContext.setOutputCardinality(outputIndex, newCardinality);
        }
    }

}
