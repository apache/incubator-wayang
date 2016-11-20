package org.qcri.rheem.profiler.log;

import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.costs.EstimationContext;

import java.util.Collection;

/**
 * {@link EstimationContext} implementation for {@link DynamicLoadEstimator}s.
 */
public class DynamicEstimationContext implements EstimationContext {

    private final EstimationContext wrappedEstimationContext;

    private final Individual individual;

    public DynamicEstimationContext(Individual individual, EstimationContext wrappedEstimationContext) {
        this.individual = individual;
        this.wrappedEstimationContext = wrappedEstimationContext;
    }

    @Override
    public CardinalityEstimate[] getInputCardinalities() {
        return this.wrappedEstimationContext.getInputCardinalities();
    }

    @Override
    public CardinalityEstimate[] getOutputCardinalities() {
        return this.wrappedEstimationContext.getOutputCardinalities();
    }

    @Override
    public double getDoubleProperty(String propertyKey, double fallback) {
        return this.wrappedEstimationContext.getDoubleProperty(propertyKey, fallback);
    }

    @Override
    public int getNumExecutions() {
        return this.wrappedEstimationContext.getNumExecutions();
    }

    @Override
    public Collection<String> getPropertyKeys() {
        return this.wrappedEstimationContext.getPropertyKeys();
    }

    public Individual getIndividual() {
        return this.individual;
    }
}
