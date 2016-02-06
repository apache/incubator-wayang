package org.qcri.rheem.core.function;

import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.costs.LoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadEstimate;
import org.qcri.rheem.core.plan.InputSlot;
import org.qcri.rheem.core.plan.OutputSlot;

/**
 * A function operates on single data units or collections of those.
 */
public abstract class FunctionDescriptor {

    protected final LoadEstimator cpuLoadEstimator;

    protected final LoadEstimator memoryLoadEstimator;

    public FunctionDescriptor(LoadEstimator cpuLoadEstimator, LoadEstimator memoryLoadEstimator) {
        this.cpuLoadEstimator = cpuLoadEstimator;
        this.memoryLoadEstimator = memoryLoadEstimator;
    }


    /**
     * Estimate the CPU usage of this instance.
     *
     * @param inputCardinalities  input {@link CardinalityEstimate}s; ordered by this instance's {@link InputSlot}s
     * @param outputCardinalities output {@link CardinalityEstimate}s; ordered by this instance's {@link OutputSlot}s
     * @return a {@link LoadEstimate}
     */
    public LoadEstimate estimateCpuUsage(CardinalityEstimate[] inputCardinalities,
                                         CardinalityEstimate[] outputCardinalities) {
        return this.cpuLoadEstimator.calculate(inputCardinalities, outputCardinalities);
    }

    /**
     * Estimate the RAM usage of this instance.
     *
     * @param inputCardinalities  input {@link CardinalityEstimate}s; ordered by this instance's {@link InputSlot}s
     * @param outputCardinalities output {@link CardinalityEstimate}s; ordered by this instance's {@link OutputSlot}s
     * @return a {@link LoadEstimate}
     */
    public LoadEstimate estimateRamUsage(CardinalityEstimate[] inputCardinalities,
                                         CardinalityEstimate[] outputCardinalities) {
        return this.memoryLoadEstimator.calculate(inputCardinalities, outputCardinalities);
    }

}
