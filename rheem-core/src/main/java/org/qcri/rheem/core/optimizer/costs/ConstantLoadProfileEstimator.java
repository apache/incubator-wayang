package org.qcri.rheem.core.optimizer.costs;

import java.util.Collection;
import java.util.LinkedList;
import java.util.function.Function;

/**
 * {@link LoadProfileEstimator} that estimates a predefined {@link LoadProfile}.
 */
public class ConstantLoadProfileEstimator implements LoadProfileEstimator {

    /**
     * The constant {@link LoadProfile} to estimate.
     */
    private final LoadProfile loadProfile;

    /**
     * Nested {@link LoadProfileEstimator}s together with a {@link Function} to extract the estimated
     * {@code Artifact} from the {@code Artifact}s subject to this instance.
     */
    private Collection<LoadProfileEstimator> nestedEstimators = new LinkedList<>();

    public ConstantLoadProfileEstimator(LoadProfile loadProfile) {
        this.loadProfile = loadProfile;
    }

    @Override
    public void nest(LoadProfileEstimator nestedEstimator) {
        this.nestedEstimators.add(nestedEstimator);
    }

    @Override
    public LoadProfile estimate(EstimationContext context) {
        return this.loadProfile;
    }

    @Override
    public Collection<LoadProfileEstimator> getNestedEstimators() {
        return this.nestedEstimators;
    }

    @Override
    public String getConfigurationKey() {
        return null;
    }

    @Override
    public LoadProfileEstimator copy() {
        // No copying required.
        return this;
    }
}
