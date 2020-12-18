package org.apache.incubator.wayang.core.optimizer.cardinality;

import org.apache.incubator.wayang.core.optimizer.OptimizationContext;
import org.apache.incubator.wayang.core.plan.wayangplan.InputSlot;
import org.apache.incubator.wayang.core.plan.wayangplan.Operator;
import org.apache.incubator.wayang.core.plan.wayangplan.OutputSlot;

/**
 * Computes {@link CardinalityEstimate}s.
 * <p>To estimate the cardinality of an {@link Operator}'s {@link OutputSlot} given its {@link InputSlot}s' cardinalities
 * is the job of this estimator.</p>
 */
@FunctionalInterface
public interface CardinalityEstimator {

    CardinalityEstimate estimate(OptimizationContext optimizationContext, CardinalityEstimate... inputEstimates);

}
