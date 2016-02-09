package org.qcri.rheem.core.optimizer.cardinality;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Objects;

public abstract class CardinalityPusher {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    protected final Operator operator;

    protected CardinalityPusher(Operator operator) {
        this.operator = operator;
    }

    public CardinalityEstimate[] push(Configuration configuration, CardinalityEstimate... inputEstimates) {
        this.logger.trace("Pushing {} into {}.", Arrays.toString(inputEstimates), this.getOperator());
        if (!canHandle(configuration, inputEstimates)) {
            this.logger.debug("Pushed incomplete estimates to {}... providing fallback estimates.",
                    this.getOperator());
            return this.createFallbackEstimates(configuration, inputEstimates);
        }
        final CardinalityEstimate[] cardinalityEstimates = this.doPush(configuration, inputEstimates);
        associateToSlots(cardinalityEstimates);
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
        return operator;
    }


}
