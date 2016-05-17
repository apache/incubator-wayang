package org.qcri.rheem.core.optimizer.cardinality;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.rheemplan.*;
import org.qcri.rheem.core.platform.ExecutionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;

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

    public void pushCardinalities() {
        this.getPlanTraversal().traverse(this.optimizationContext, this.configuration);
        this.optimizationContext.clearMarks();
        assert this.optimizationContext.isTimeEstimatesComplete();
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
     */
    public void pushCardinalityUpdates(ExecutionState executionState) {
        this.injectMeasuredCardinalities(executionState);
        this.pushCardinalities();
    }

    /**
     * Injects the cardinalities of a current {@link ExecutionState} into its associated {@link RheemPlan}.
     */
    private void injectMeasuredCardinalities(ExecutionState executionState) {
        executionState.getCardinalityMeasurements().forEach(this::injectMeasureCardinality);
    }

    /**
     * Injects the measured {@code cardinality} of a {@code channel} into the {@link #optimizationContext}.
     */
    private void injectMeasureCardinality(Channel channel, long cardinality) {
        // Build the new CardinalityEstimate.
        final CardinalityEstimate newEstimate = new CardinalityEstimate(cardinality, cardinality, 1d);

        // Identify the Slots that correspond to the channel.
        final Collection<Slot<?>> correspondingSlots = channel.getCorrespondingSlots();
        for (Slot<?> correspondingSlot : correspondingSlots) {

            // Identify the corresponding OperatorContext.
            final Operator owner = correspondingSlot.getOwner();
            final OptimizationContext.OperatorContext operatorCtx = this.optimizationContext.getOperatorContext(owner);
            if (operatorCtx == null) {
                // FIXME: Within loops, we might need to propagate across iterations!
                this.logger.debug("Could not inject measured cardinality for {}: It is presumably a glue operator or inside of a loop.", owner);
                continue;
            }

            // Update the operatorCtx, then propagate.
            // TODO: Do we need to propagate? Revisit once channel.getCorrespondingSlots() is incorporating OptimizationContexts.
            final int slotIndex = correspondingSlot.getIndex();
            if (correspondingSlot instanceof InputSlot<?>) {
                operatorCtx.setInputCardinality(slotIndex, newEstimate);
                owner.propagateInputCardinality(slotIndex, operatorCtx);
            } else {
                assert correspondingSlot instanceof OutputSlot<?>;
                operatorCtx.setOutputCardinality(slotIndex, newEstimate);
                owner.propagateOutputCardinality(slotIndex, operatorCtx);
            }
        }
    }

}
