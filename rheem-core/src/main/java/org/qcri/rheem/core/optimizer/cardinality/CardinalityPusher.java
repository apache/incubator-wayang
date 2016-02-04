package org.qcri.rheem.core.optimizer.cardinality;

import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.plan.Operator;
import org.qcri.rheem.core.plan.OutputSlot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

public abstract class CardinalityPusher {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    protected final Map<OutputSlot<?>, CardinalityEstimate> cache;

    protected final Operator operator;

    protected CardinalityPusher(Operator operator, Map<OutputSlot<?>, CardinalityEstimate> cache) {
        this.operator = operator;
        this.cache = cache;
    }

    public CardinalityEstimate[] push(RheemContext rheemContext, CardinalityEstimate... inputEstimates) {
        this.logger.trace("Pushing {} into {}.", Arrays.toString(inputEstimates), this.getOperator());
        if (!canHandle(rheemContext, inputEstimates)) {
            this.logger.info("Pushed incomplete estimates to {}... providing fallback estimates.",
                    this.getOperator());
            return this.createFallbackEstimates(rheemContext, inputEstimates);
        }

        return this.doPush(rheemContext, inputEstimates);
    }

    private boolean canHandle(RheemContext rheemContext, CardinalityEstimate[] inputEstimates) {
        return Arrays.stream(inputEstimates).noneMatch(Objects::isNull);
    }

    private CardinalityEstimate[] createFallbackEstimates(RheemContext rheemContext, CardinalityEstimate[] inputEstimates) {
        return new CardinalityEstimate[this.getOperator().getNumOutputs()];
    }

    protected abstract CardinalityEstimate[] doPush(RheemContext rheemContext, CardinalityEstimate... inputEstimates);

    public Operator getOperator() {
        return operator;
    }


}
