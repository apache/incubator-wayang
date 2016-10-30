package org.qcri.rheem.core.optimizer.cardinality;

import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;

/**
 * Computes {@link CardinalityEstimate}s.
 * <p>To estimate the cardinality of an {@link Operator}'s {@link OutputSlot} given its {@link InputSlot}s' cardinalities
 * is the job of this estimator.</p>
 */
@FunctionalInterface
public interface CardinalityEstimator {

    CardinalityEstimate estimate(OptimizationContext optimizationContext, CardinalityEstimate... inputEstimates);

}
