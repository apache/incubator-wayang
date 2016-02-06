package org.qcri.rheem.core.optimizer.cardinality;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.plan.InputSlot;
import org.qcri.rheem.core.plan.Operator;
import org.qcri.rheem.core.plan.OutputSlot;

/**
 * Computes {@link CardinalityEstimate}s.
 * <p>To estimate the cardinality of an {@link Operator}'s {@link OutputSlot} given its {@link InputSlot}s' cardinalities
 * is the job of this estimator.</p>
 */
@FunctionalInterface
public interface CardinalityEstimator {

    CardinalityEstimate estimate(Configuration configuration, CardinalityEstimate... inputEstimates);

}
