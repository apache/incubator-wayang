package org.qcri.rheem.core.plan;

import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.costs.ResourceFunction;
import org.qcri.rheem.core.optimizer.costs.ResourceUsageEstimate;
import org.qcri.rheem.core.platform.Platform;

import java.util.Collection;
import java.util.Collections;

/**
 * An execution operator is handled by a certain platform.
 */
public interface ExecutionOperator extends ActualOperator {

    /**
     * @return the platform that can run this operator
     */
    Platform getPlatform();

    /**
     * @return a copy of this instance; it's {@link Slot}s will not be connected
     */
    ExecutionOperator copy();

    default ResourceFunction getCpuResourceFunction() {
        return ResourceFunction.createFallback(this.getNumInputs(), this.getNumOutputs());
    }

    default ResourceFunction getRamResourceFunction() {
        return ResourceFunction.createFallback(this.getNumInputs(), this.getNumOutputs());
    }

    default ResourceFunction getDiskResourceFunction() {
        return ResourceFunction.createFallback(this.getNumInputs(), this.getNumOutputs());
    }

    default ResourceFunction getNetworkResourceFunction() {
        return ResourceFunction.createFallback(this.getNumInputs(), this.getNumOutputs());
    }

    /**
     * Estimate the CPU usage of this instance.
     *
     * @param inputCardinalities  input {@link CardinalityEstimate}s; ordered by this instance's {@link InputSlot}s
     * @param outputCardinalities output {@link CardinalityEstimate}s; ordered by this instance's {@link OutputSlot}s
     * @return a {@link ResourceUsageEstimate}
     */
    default ResourceUsageEstimate estimateCpuUsage(CardinalityEstimate[] inputCardinalities,
                                           CardinalityEstimate[] outputCardinalities) {
        return this.getCpuResourceFunction().calculate(inputCardinalities, outputCardinalities);
    }

    /**
     * Estimate the RAM usage of this instance.
     *
     * @param inputCardinalities  input {@link CardinalityEstimate}s; ordered by this instance's {@link InputSlot}s
     * @param outputCardinalities output {@link CardinalityEstimate}s; ordered by this instance's {@link OutputSlot}s
     * @return a {@link ResourceUsageEstimate}
     */
    default ResourceUsageEstimate estimateRamUsage(CardinalityEstimate[] inputCardinalities,
                                           CardinalityEstimate[] outputCardinalities) {
        return this.getRamResourceFunction().calculate(inputCardinalities, outputCardinalities);
    }
    /**
     * Estimate the disk usage of this instance.
     *
     * @param inputCardinalities  input {@link CardinalityEstimate}s; ordered by this instance's {@link InputSlot}s
     * @param outputCardinalities output {@link CardinalityEstimate}s; ordered by this instance's {@link OutputSlot}s
     * @return a {@link ResourceUsageEstimate}
     */
    default ResourceUsageEstimate estimateDiskUsage(CardinalityEstimate[] inputCardinalities,
                                           CardinalityEstimate[] outputCardinalities) {
        return this.getDiskResourceFunction().calculate(inputCardinalities, outputCardinalities);
    }
    /**
     * Estimate the network usage of this instance.
     *
     * @param inputCardinalities  input {@link CardinalityEstimate}s; ordered by this instance's {@link InputSlot}s
     * @param outputCardinalities output {@link CardinalityEstimate}s; ordered by this instance's {@link OutputSlot}s
     * @return a {@link ResourceUsageEstimate}
     */
    default ResourceUsageEstimate estimateNetworkUsage(CardinalityEstimate[] inputCardinalities,
                                           CardinalityEstimate[] outputCardinalities) {
        return this.getNetworkResourceFunction().calculate(inputCardinalities, outputCardinalities);
    }

    default Collection<FunctionDescriptor> getAllFunctionDescriptors() {
        return Collections.emptyList();
    }

}
