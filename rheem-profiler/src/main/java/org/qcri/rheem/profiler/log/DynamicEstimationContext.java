package org.qcri.rheem.profiler.log;

import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.costs.EstimationContext;
import org.qcri.rheem.core.platform.PartialExecution;

/**
 * {@link EstimationContext} implementation for {@link DynamicLoadEstimator}s.
 */
public class DynamicEstimationContext implements EstimationContext {

    private final PartialExecution.OperatorExecution operatorExecution;

    private final Individual individual;

    public DynamicEstimationContext(Individual individual, PartialExecution.OperatorExecution operatorExecution) {
        this.individual = individual;
        this.operatorExecution = operatorExecution;
    }

    @Override
    public CardinalityEstimate[] getInputCardinalities() {
        return this.operatorExecution.getInputCardinalities();
    }

    @Override
    public CardinalityEstimate[] getOutputCardinalities() {
        return this.operatorExecution.getOutputCardinalities();
    }

    @Override
    public double getDoubleProperty(String propertyKey, double fallback) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public int getNumExecutions() {
        return this.operatorExecution.getNumExecutions();
    }

    public Individual getIndividual() {
        return this.individual;
    }
}
