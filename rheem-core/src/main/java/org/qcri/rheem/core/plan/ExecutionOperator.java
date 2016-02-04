package org.qcri.rheem.core.plan;

import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.costs.ResourceUsageEstimate;
import org.qcri.rheem.core.platform.Platform;

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

    /**
     * Estimate the CPU usage of this instance.
     *
     * @param inputCardinalities  input {@link CardinalityEstimate}s; ordered by this instance's {@link InputSlot}s
     * @param outputCardinalities output {@link CardinalityEstimate}s; ordered by this instance's {@link OutputSlot}s
     * @return a {@link ResourceUsageEstimate}
     */
    ResourceUsageEstimate estimateCpuUsage(CardinalityEstimate[] inputCardinalities,
                                           CardinalityEstimate[] outputCardinalities);

    /**
     * Estimate the RAM usage of this instance.
     *
     * @param inputCardinalities  input {@link CardinalityEstimate}s; ordered by this instance's {@link InputSlot}s
     * @param outputCardinalities output {@link CardinalityEstimate}s; ordered by this instance's {@link OutputSlot}s
     * @return a {@link ResourceUsageEstimate}
     */
    ResourceUsageEstimate estimateRamUsage(CardinalityEstimate[] inputCardinalities,
                                           CardinalityEstimate[] outputCardinalities);
    /**
     * Estimate the disk usage of this instance.
     *
     * @param inputCardinalities  input {@link CardinalityEstimate}s; ordered by this instance's {@link InputSlot}s
     * @param outputCardinalities output {@link CardinalityEstimate}s; ordered by this instance's {@link OutputSlot}s
     * @return a {@link ResourceUsageEstimate}
     */
    ResourceUsageEstimate estimateDiskUsage(CardinalityEstimate[] inputCardinalities,
                                           CardinalityEstimate[] outputCardinalities);
    /**
     * Estimate the network usage of this instance.
     *
     * @param inputCardinalities  input {@link CardinalityEstimate}s; ordered by this instance's {@link InputSlot}s
     * @param outputCardinalities output {@link CardinalityEstimate}s; ordered by this instance's {@link OutputSlot}s
     * @return a {@link ResourceUsageEstimate}
     */
    ResourceUsageEstimate estimateNetworkUsage(CardinalityEstimate[] inputCardinalities,
                                           CardinalityEstimate[] outputCardinalities);

}
