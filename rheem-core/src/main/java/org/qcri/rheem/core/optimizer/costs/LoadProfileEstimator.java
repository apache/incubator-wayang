package org.qcri.rheem.core.optimizer.costs;

import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;

/**
 * Estimates the {@link LoadProfile} of some executable artifact that takes some input data quanta and produces them.
 */
public interface LoadProfileEstimator<Artifact> {

    /**
     * Estimates a {@link LoadProfile}.
     *
     * @param artifact        the artifact whose {@link LoadProfile} should be produced
     * @param inputEstimates  {@link CardinalityEstimate}s for data quanta arriving at the various input slots of the {@code artifact}
     * @param outputEstimates {@link CardinalityEstimate}s for data quanta departing at the various output slots of the {@code artifact}
     * @return the {@link LoadProfile}
     */
    LoadProfile estimate(Artifact artifact, CardinalityEstimate[] inputEstimates, CardinalityEstimate[] outputEstimates);

}
