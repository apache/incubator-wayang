package org.qcri.rheem.core.optimizer.cardinality;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.OptimizationUtils;
import org.qcri.rheem.core.optimizer.enumeration.LoopImplementation;
import org.qcri.rheem.core.optimizer.enumeration.PlanImplementation;
import org.qcri.rheem.core.plan.rheemplan.LoopSubplan;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.ExecutionState;
import org.qcri.rheem.core.platform.Junction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

/**
 * Handles the {@link CardinalityEstimate}s of a {@link RheemPlan}.
 */
public class CardinalityEstimatorManager {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * The {@link RheemPlan} whose cardinalities are being managed.
     */
    private final RheemPlan rheemPlan;

    /**
     * Keeps the {@link CardinalityEstimate}s around.
     */
    private final OptimizationContext optimizationContext;

    /**
     * Provides {@link CardinalityEstimator}s etc.
     */
    private final Configuration configuration;

    private CardinalityEstimationTraversal planTraversal;

    public CardinalityEstimatorManager(RheemPlan rheemPlan,
                                       OptimizationContext optimizationContext,
                                       Configuration configuration) {
        this.rheemPlan = rheemPlan;
        this.optimizationContext = optimizationContext;
        this.configuration = configuration;
    }

    /**
     * Traverse the {@link RheemPlan}, thereby updating {@link CardinalityEstimate}s.
     *
     * @return whether any {@link CardinalityEstimate}s have been updated
     */
    public boolean pushCardinalities() {
        boolean isUpdated = this.getPlanTraversal().traverse(this.optimizationContext, this.configuration);
        this.optimizationContext.clearMarks();
        return isUpdated;
    }

    /**
     * Traverse the {@link RheemPlan}, thereby updating {@link CardinalityEstimate}s. Also update conversion
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
                    this.rheemPlan.collectReachableTopLevelSources(),
                    this.configuration
            );
        }
        return this.planTraversal;
    }

    /**
     * Injects the cardinalities of a current {@link ExecutionState} into its associated {@link RheemPlan}
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
     * Injects the cardinalities of a current {@link ExecutionState} into its associated {@link RheemPlan}.
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

        // Try to inject into the RheemPlan Operator output.
        final OutputSlot<?> rheemPlanOutput = OptimizationUtils.findRheemPlanOutputSlotFor(channelInstance.getChannel());
        int outputIndex = rheemPlanOutput == null ? 0 : rheemPlanOutput.getIndex();
        OptimizationContext optimizationContext = channelInstance.getProducerOperatorContext().getOptimizationContext();
        final OptimizationContext.OperatorContext rheemPlanOperatorCtx = optimizationContext.getOperatorContext(rheemPlanOutput.getOwner());
        if (rheemPlanOperatorCtx != null) {
            this.injectMeasuredCardinality(cardinality, rheemPlanOperatorCtx, outputIndex);
        } else {
            this.logger.warn("Could not inject cardinality measurement {} for {}.", cardinality, rheemPlanOutput);
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
