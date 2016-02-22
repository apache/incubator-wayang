package org.qcri.rheem.core.optimizer.cardinality;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.plan.rheemplan.*;
import org.qcri.rheem.core.platform.CrossPlatformExecutor;

import java.util.Collection;

/**
 * TODO
 */
public class CardinalityEstimatorManager {

    private final Configuration configuration;

    public CardinalityEstimatorManager(Configuration configuration) {
        this.configuration = configuration;
    }

    public void pushCardinalityEstimation(RheemPlan rheemPlan) {
        final CardinalityPusher pusher = CompositeCardinalityPusher.createFor(rheemPlan, this.configuration);
        pusher.push(this.configuration);
    }

    /**
     * Injects the cardinalities of a current {@link CrossPlatformExecutor.State} into its associated {@link RheemPlan}
     * and then reperforms the cardinality estimation.
     */
    public void pushCardinalityUpdates(RheemPlan rheemPlan, CrossPlatformExecutor.State executionState) {
        this.injectMeasuredCardinalities(executionState);
        this.pushCardinalityEstimation(rheemPlan);
    }

    /**
     * Injects the cardinalities of a current {@link CrossPlatformExecutor.State} into its associated {@link RheemPlan}.
     */
    private void injectMeasuredCardinalities(CrossPlatformExecutor.State executionState) {
        executionState.getProfile().getCardinalities().forEach(
                (channel, cardinality) -> {
                    final CardinalityEstimate newEstimate = new CardinalityEstimate(cardinality, cardinality, 1d);
                    final Collection<Slot<?>> correspondingSlots = channel.getCorrespondingSlots();
                    for (Slot<?> correspondingSlot : correspondingSlots) {
                        final Operator owner = correspondingSlot.getOwner();
                        final int slotIndex = correspondingSlot.getIndex();
                        if (correspondingSlot instanceof InputSlot<?>) {
                            owner.propagateInputCardinality(slotIndex, newEstimate);
                        } else {
                            assert correspondingSlot instanceof OutputSlot<?>;
                            owner.propagateOutputCardinality(slotIndex, newEstimate);
                        }
                    }
                }
        );
    }

}
