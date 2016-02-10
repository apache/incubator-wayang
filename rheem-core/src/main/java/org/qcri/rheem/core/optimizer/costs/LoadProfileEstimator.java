package org.qcri.rheem.core.optimizer.costs;

import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;

/**
 * Estimates the {@link LoadProfile} of some executable artifact.
 */
public interface LoadProfileEstimator {

    LoadProfile estimate(CardinalityEstimate[] inputEstimates, CardinalityEstimate[] outputEstimates);

}
