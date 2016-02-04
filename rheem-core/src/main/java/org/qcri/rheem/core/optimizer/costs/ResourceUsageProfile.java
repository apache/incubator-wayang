package org.qcri.rheem.core.optimizer.costs;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.plan.ExecutionOperator;
import org.qcri.rheem.core.plan.InputSlot;
import org.qcri.rheem.core.plan.Operator;
import org.qcri.rheem.core.plan.OutputSlot;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Reflects the (estimated) required resources of an {@link Operator} or {@link FunctionDescriptor}.
 */
public class ResourceUsageProfile {

    private final ResourceUsageEstimate cpuUsage, ramUsage, networkUsage, diskUsage;

    private final Collection<ResourceUsageProfile> subprofiles = new LinkedList<>();

    private ResourceUsageProfile(ResourceUsageEstimate cpuUsage,
                                 ResourceUsageEstimate ramUsage,
                                 ResourceUsageEstimate networkUsage,
                                 ResourceUsageEstimate diskUsage) {
        this.cpuUsage = cpuUsage;
        this.ramUsage = ramUsage;
        this.networkUsage = networkUsage;
        this.diskUsage = diskUsage;
    }

    private ResourceUsageProfile(ResourceUsageEstimate cpuUsage, ResourceUsageEstimate ramUsage) {
        this(cpuUsage, ramUsage, null, null);
    }

    /**
     * Create a new instance for an {@link ExecutionOperator} in the context of a plan.
     *
     * @param executionOperator    whose {@link ResourceUsageProfile} should be established
     * @param cardinalityEstimates cache of all {@link CardinalityEstimate}s of the plan that embed the {@code executionOperator}
     * @return the new instance
     * @throws IllegalArgumentException if a required {@link CardinalityEstimate} is missing
     */
    public static ResourceUsageProfile createFor(ExecutionOperator executionOperator,
                                                 Map<OutputSlot<?>, CardinalityEstimate> cardinalityEstimates)
            throws IllegalArgumentException {

        CardinalityEstimate[] inputEstimates = collectInputEstimates(executionOperator, cardinalityEstimates);
        CardinalityEstimate[] outputEstimates = collectOutputEstimates(executionOperator, cardinalityEstimates);

        final ResourceUsageProfile operatorProfile = new ResourceUsageProfile(
                executionOperator.estimateCpuUsage(inputEstimates, outputEstimates),
                executionOperator.estimateRamUsage(inputEstimates, outputEstimates),
                executionOperator.estimateNetworkUsage(inputEstimates, outputEstimates),
                executionOperator.estimateDiskUsage(inputEstimates, outputEstimates)
        );

        executionOperator.getAllFunctionDescriptors().stream()
                .map(fd -> createFor(fd, executionOperator, cardinalityEstimates))
                .forEach(operatorProfile.subprofiles::add);
        return operatorProfile;
    }

    /**
     * Create a new instance for a {@link FunctionDescriptor} in the context of an {@link ExecutionOperator}.
     *
     * @param executionOperator    whose {@link ResourceUsageProfile} should be established
     * @param cardinalityEstimates cache of all {@link CardinalityEstimate}s of the plan that embed the {@code executionOperator}
     * @return the new instance
     * @throws IllegalArgumentException if a required {@link CardinalityEstimate} is missing
     */
    public static ResourceUsageProfile createFor(FunctionDescriptor functionDescriptor,
                                                 ExecutionOperator executionOperator,
                                                 Map<OutputSlot<?>, CardinalityEstimate> cardinalityEstimates)
            throws IllegalArgumentException {

        CardinalityEstimate[] inputEstimates = collectInputEstimates(executionOperator, cardinalityEstimates);
        CardinalityEstimate[] outputEstimates = collectOutputEstimates(executionOperator, cardinalityEstimates);

        return new ResourceUsageProfile(
                functionDescriptor.estimateCpuUsage(inputEstimates, outputEstimates),
                functionDescriptor.estimateRamUsage(inputEstimates, outputEstimates)
        );
    }

    private static CardinalityEstimate[] collectInputEstimates(ExecutionOperator executionOperator,
                                                               Map<OutputSlot<?>, CardinalityEstimate> cardinalityEstimates) {
        final InputSlot<?>[] operatorInputs = executionOperator.getAllInputs();
        CardinalityEstimate[] collectedEstimates = new CardinalityEstimate[operatorInputs.length];
        for (int inputIndex = 0; inputIndex < operatorInputs.length; inputIndex++) {
            final InputSlot<?> input = operatorInputs[inputIndex];
            final InputSlot<?> outermostInput = executionOperator.getOutermostInputSlot(input);
            final OutputSlot<?> occupant = outermostInput.getOccupant();
            collectedEstimates[inputIndex] = cardinalityEstimates.get(occupant);
            Validate.notNull(collectedEstimates[inputIndex],
                    "Could not find a cardinality estimate for input %d of %s (looked at %s).",
                    inputIndex, executionOperator, occupant);
        }
        return collectedEstimates;
    }

    private static CardinalityEstimate[] collectOutputEstimates(ExecutionOperator executionOperator,
                                                                Map<OutputSlot<?>, CardinalityEstimate> cardinalityEstimates) {
        final OutputSlot<?>[] operatorOutputs = executionOperator.getAllOutputs();
        CardinalityEstimate[] collectedEstimates = new CardinalityEstimate[operatorOutputs.length];
        for (int outputIndex = 0; outputIndex < operatorOutputs.length; outputIndex++) {
            final OutputSlot<?> output = operatorOutputs[outputIndex];
            final Collection<OutputSlot<Object>> outermostOutputs = executionOperator.getOutermostOutputSlots(output.unchecked());
            final Set<CardinalityEstimate> estimates = outermostOutputs.stream()
                    .map(cardinalityEstimates::get)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet());
            Validate.isTrue(estimates.size() == 1,
                    "Illegal number of cardinality estimates for %s: %s.",
                    executionOperator, estimates);
            collectedEstimates[outputIndex] = estimates.stream().findFirst().get();
        }
        return collectedEstimates;
    }

    public ResourceUsageEstimate getCpuUsage() {
        return cpuUsage;
    }

    public ResourceUsageEstimate getRamUsage() {
        return ramUsage;
    }

    public ResourceUsageEstimate getNetworkUsage() {
        return networkUsage;
    }

    public ResourceUsageEstimate getDiskUsage() {
        return diskUsage;
    }

    public Collection<ResourceUsageProfile> getSubprofiles() {
        return subprofiles;
    }
}
