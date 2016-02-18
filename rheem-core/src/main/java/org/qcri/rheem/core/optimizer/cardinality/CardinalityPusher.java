package org.qcri.rheem.core.optimizer.cardinality;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Objects;

/**
 * Pushes a input {@link CardinalityEstimate}s through an {@link Operator} and yields its output
 * {@link CardinalityEstimate}s. As an important side-effect, {@link Operator}s will store their
 * {@link CardinalityEstimate}s.
 */
public abstract class CardinalityPusher {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected final Operator operator;

    protected CardinalityPusher(Operator operator) {
        this.operator = operator;
    }

    public CardinalityEstimate[] push(Configuration configuration, CardinalityEstimate... inputEstimates) {
        this.logger.trace("Pushing {} into {}.", Arrays.toString(inputEstimates), this.getOperator());
        if (!this.canHandle(configuration, inputEstimates)) {
            this.logger.debug("Pushed incomplete estimates to {}... providing fallback estimates.",
                    this.getOperator());
            return this.createFallbackEstimates(configuration, inputEstimates);
        }
        final CardinalityEstimate[] cardinalityEstimates = this.doPush(configuration, inputEstimates);
        this.associateToSlots(cardinalityEstimates);
        return cardinalityEstimates;
    }

    private void associateToSlots(CardinalityEstimate[] cardinalityEstimates) {
        for (int outputIndex = 0; outputIndex < this.getOperator().getNumOutputs(); outputIndex++) {
            final OutputSlot<?> output = this.operator.getOutput(outputIndex);
            final CardinalityEstimate cardinalityEstimate = cardinalityEstimates[outputIndex];
            output.setCardinalityEstimate(cardinalityEstimate);
            for (InputSlot<?> input : output.getOccupiedSlots()) {
                input.setCardinalityEstimate(cardinalityEstimate);
            }
        }
    }

    private boolean canHandle(Configuration configuration, CardinalityEstimate[] inputEstimates) {
        return Arrays.stream(inputEstimates).noneMatch(Objects::isNull);
    }

    private CardinalityEstimate[] createFallbackEstimates(Configuration configuration, CardinalityEstimate[] inputEstimates) {
        return new CardinalityEstimate[this.getOperator().getNumOutputs()];
    }

    protected abstract CardinalityEstimate[] doPush(Configuration configuration, CardinalityEstimate... inputEstimates);

    public Operator getOperator() {
        return this.operator;
    }


}
