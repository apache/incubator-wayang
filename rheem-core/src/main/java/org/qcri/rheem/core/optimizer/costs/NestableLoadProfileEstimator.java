package org.qcri.rheem.core.optimizer.costs;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.plan.rheemplan.Slot;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * {@link LoadProfileEstimator} that can host further {@link LoadProfileEstimator}s.
 */
public class NestableLoadProfileEstimator implements LoadProfileEstimator {

    private final LoadEstimator cpuLoadEstimator, ramLoadEstimator, diskLoadEstimator, networkLoadEstimator;

    private Collection<LoadProfileEstimator> nestedLoadEstimators = new LinkedList<>();

    /**
     * Creates an new instance.
     *
     * @param cpuLoadEstimator estimates CPU load in terms of cycles
     * @param ramLoadEstimator estimates RAM load in terms of MB
     */
    public NestableLoadProfileEstimator(LoadEstimator cpuLoadEstimator, LoadEstimator ramLoadEstimator) {
        this(cpuLoadEstimator, ramLoadEstimator, null, null);
    }

    /**
     * Creates an new instance.
     *
     * @param cpuLoadEstimator     estimates CPU load in terms of cycles
     * @param ramLoadEstimator     estimates RAM load in terms of bytes
     * @param diskLoadEstimator    estimates disk accesses in terms of bytes
     * @param networkLoadEstimator estimates network in terms of bytes
     */
    public NestableLoadProfileEstimator(LoadEstimator cpuLoadEstimator,
                                        LoadEstimator ramLoadEstimator,
                                        LoadEstimator diskLoadEstimator,
                                        LoadEstimator networkLoadEstimator) {
        this.cpuLoadEstimator = cpuLoadEstimator;
        this.ramLoadEstimator = ramLoadEstimator;
        this.diskLoadEstimator = diskLoadEstimator;
        this.networkLoadEstimator = networkLoadEstimator;
    }

    public void nest(LoadProfileEstimator nestedEstimator) {
        this.nestedLoadEstimators.add(nestedEstimator);
    }

    @Override
    public LoadProfile estimate(ExecutionOperator executionOperator) {
        CardinalityEstimate[] inputEstimates = collectInputEstimates(executionOperator);
        CardinalityEstimate[] outputEstimates = collectOutputEstimates(executionOperator);
        return this.estimate(inputEstimates, outputEstimates);
    }

    /**
     * Assemble the input {@link CardinalityEstimate} array for a given {@link ExecutionOperator} from a precalculated
     * {@link CardinalityEstimate}s.
     */
    private static CardinalityEstimate[] collectInputEstimates(ExecutionOperator executionOperator) {
        final InputSlot<?>[] operatorInputs = executionOperator.getAllInputs();
        CardinalityEstimate[] collectedEstimates = new CardinalityEstimate[operatorInputs.length];
        for (int inputIndex = 0; inputIndex < operatorInputs.length; inputIndex++) {
            final InputSlot<?> input = operatorInputs[inputIndex];
            final InputSlot<?> outermostInput = executionOperator.getOutermostInputSlot(input);
            collectedEstimates[inputIndex] = outermostInput.getCardinalityEstimate();
            Validate.notNull(collectedEstimates[inputIndex],
                    "Could not find a cardinality estimate for input %d of %s (looked at %s).",
                    inputIndex, executionOperator, outermostInput);
        }
        return collectedEstimates;
    }

    /**
     * Assemble the output {@link CardinalityEstimate} array for a given {@link ExecutionOperator} from a precalculated
     * {@link CardinalityEstimate}s.
     */
    private static CardinalityEstimate[] collectOutputEstimates(ExecutionOperator executionOperator) {
        final OutputSlot<?>[] operatorOutputs = executionOperator.getAllOutputs();
        CardinalityEstimate[] collectedEstimates = new CardinalityEstimate[operatorOutputs.length];
        for (int outputIndex = 0; outputIndex < operatorOutputs.length; outputIndex++) {
            final OutputSlot<?> output = operatorOutputs[outputIndex];
            final Collection<OutputSlot<Object>> outermostOutputs = executionOperator.getOutermostOutputSlots(output.unchecked());
            final Set<CardinalityEstimate> estimates = outermostOutputs.stream()
                    .map(Slot::getCardinalityEstimate)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet());
            Validate.isTrue(estimates.size() == 1,
                    "Illegal number of cardinality estimates for %s: %s.",
                    executionOperator, estimates);
            collectedEstimates[outputIndex] = estimates.stream().findFirst().get();
        }
        return collectedEstimates;
    }

    @Override
    public LoadProfile estimate(CardinalityEstimate[] inputEstimates, CardinalityEstimate[] outputEstimates) {
        final LoadProfile mainLoadProfile = this.performLocalEstimation(inputEstimates, outputEstimates);
        this.performNestedEstimations(inputEstimates, outputEstimates, mainLoadProfile);
        return mainLoadProfile;
    }

    private LoadProfile performLocalEstimation(CardinalityEstimate[] inputEstimates, CardinalityEstimate[] outputEstimates) {
        final LoadEstimate cpuLoadEstimate = this.cpuLoadEstimator.calculate(inputEstimates, outputEstimates);
        final LoadEstimate ramLoadEstimate = this.ramLoadEstimator.calculate(inputEstimates, outputEstimates);
        final LoadEstimate diskLoadEstimate = this.diskLoadEstimator == null ? null :
                this.diskLoadEstimator.calculate(inputEstimates, outputEstimates);
        final LoadEstimate networkLoadEstimate = this.networkLoadEstimator == null ? null :
                this.networkLoadEstimator.calculate(inputEstimates, outputEstimates);
        return new LoadProfile(cpuLoadEstimate, ramLoadEstimate, networkLoadEstimate, diskLoadEstimate);
    }

    private void performNestedEstimations(CardinalityEstimate[] inputEstimates, CardinalityEstimate[] outputEstimates, LoadProfile mainLoadProfile) {
        for (LoadProfileEstimator nestedLoadEstimator : this.nestedLoadEstimators) {
            final LoadProfile subprofile = nestedLoadEstimator.estimate(inputEstimates, outputEstimates);
            mainLoadProfile.nest(subprofile);
        }
    }


}
