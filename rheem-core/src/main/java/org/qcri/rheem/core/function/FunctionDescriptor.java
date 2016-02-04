package org.qcri.rheem.core.function;

import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.costs.ResourceFunction;
import org.qcri.rheem.core.optimizer.costs.ResourceUsageEstimate;
import org.qcri.rheem.core.plan.InputSlot;
import org.qcri.rheem.core.plan.OutputSlot;

/**
 * A function operates on single data units or collections of those.
 */
public abstract class FunctionDescriptor {

    protected final ResourceFunction cpuResourceFunction;

    protected final ResourceFunction memoryResourceFunction;

    public FunctionDescriptor(ResourceFunction cpuResourceFunction, ResourceFunction memoryResourceFunction) {
        this.cpuResourceFunction = cpuResourceFunction;
        this.memoryResourceFunction = memoryResourceFunction;
    }


    /**
     * Estimate the CPU usage of this instance.
     *
     * @param inputCardinalities  input {@link CardinalityEstimate}s; ordered by this instance's {@link InputSlot}s
     * @param outputCardinalities output {@link CardinalityEstimate}s; ordered by this instance's {@link OutputSlot}s
     * @return a {@link ResourceUsageEstimate}
     */
    public ResourceUsageEstimate estimateCpuUsage(CardinalityEstimate[] inputCardinalities,
                                           CardinalityEstimate[] outputCardinalities) {
        return this.cpuResourceFunction.calculate(inputCardinalities, outputCardinalities);
    }

    /**
     * Estimate the RAM usage of this instance.
     *
     * @param inputCardinalities  input {@link CardinalityEstimate}s; ordered by this instance's {@link InputSlot}s
     * @param outputCardinalities output {@link CardinalityEstimate}s; ordered by this instance's {@link OutputSlot}s
     * @return a {@link ResourceUsageEstimate}
     */
    public ResourceUsageEstimate estimateRamUsage(CardinalityEstimate[] inputCardinalities,
                                           CardinalityEstimate[] outputCardinalities) {
        return this.memoryResourceFunction.calculate(inputCardinalities, outputCardinalities);
    }

}
