package org.qcri.rheem.core.optimizer.costs;

import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;

/**
 * Estimates the {@link LoadProfile} of some executable artifact.
 */
public interface LoadProfileEstimator {

    @Deprecated
    LoadProfile estimate(CardinalityEstimate[] inputEstimates, CardinalityEstimate[] outputEstimates);

    LoadProfile estimate(OptimizationContext.OperatorContext operatorContext);

}
