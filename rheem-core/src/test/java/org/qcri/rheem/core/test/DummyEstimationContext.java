package org.qcri.rheem.core.test;

import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.costs.EstimationContext;

/**
 * TODO
 */
public class DummyEstimationContext implements EstimationContext {

    public CardinalityEstimate[] inputCardinalities, outputCardinalities;

    int numExecutions;

    public DummyEstimationContext(CardinalityEstimate[] inputCardinalities, CardinalityEstimate[] outputCardinalities, int numExecutions) {
        this.inputCardinalities = inputCardinalities;
        this.outputCardinalities = outputCardinalities;
        this.numExecutions = numExecutions;
    }

    @Override
    public CardinalityEstimate[] getInputCardinalities() {
        return this.inputCardinalities;
    }

    @Override
    public CardinalityEstimate[] getOutputCardinalities() {
        return this.outputCardinalities;
    }

    @Override
    public double getDoubleProperty(String propertyKey, double fallback) {
        return fallback;
    }

    @Override
    public int getNumExecutions() {
        return this.numExecutions;
    }
}
