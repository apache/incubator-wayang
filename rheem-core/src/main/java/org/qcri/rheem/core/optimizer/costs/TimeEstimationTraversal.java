package org.qcri.rheem.core.optimizer.costs;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.configuration.ConfigurationProvider;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.plan.*;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Traverses a {@link PhysicalPlan}, thereby creating {@link TimeEstimate}s for every encountered operator.
 */
public class TimeEstimationTraversal {

    private final Configuration configuration;

    private final Map<OutputSlot<?>, CardinalityEstimate> cardinalityEstimates;

    private PlanTraversal planTraversal = new PlanTraversal(true, false)
            .withCallback((operator, fromInputSlot, fromOutputSlot) -> this.handleOperator(operator));

    private final Map<ExecutionOperator, TimeEstimate> timeEstimates = new HashMap<>();

    private TimeEstimationTraversal(Configuration configuration,
                                    Map<OutputSlot<?>, CardinalityEstimate> cardinalityEstimates) {
        this.configuration = configuration;
        this.cardinalityEstimates = cardinalityEstimates;
    }

    /**
     * Traverse the given plan and its subplans, thereby determining {@link TimeEstimate}s for all encountered
     * {@link ExecutionOperator}s.
     *
     * @param plan                 should be traversed
     * @param cardinalityEstimates provides {@link CardinalityEstimate}s of the {@link PhysicalPlan}
     * @return the {@link TimeEstimate}s for all encountered {@link ExecutionOperator}s
     */
    public static Map<ExecutionOperator, TimeEstimate> traverse(
            PhysicalPlan plan,
            Configuration configuration,
            Map<OutputSlot<?>, CardinalityEstimate> cardinalityEstimates) {

        TimeEstimationTraversal instance = new TimeEstimationTraversal(configuration, cardinalityEstimates);
        plan.getSinks().forEach(instance::startTraversalFrom);
        return instance.timeEstimates;
    }

    private void startTraversalFrom(Operator startOperator) {
        planTraversal.traverse(startOperator);
    }

    private void startTraversalFrom(Collection<Operator> startOperators) {
        startOperators.forEach(this::startTraversalFrom);
    }

    private void handleOperator(Operator operator) {
        if (operator.isAlternative()) {
            ((OperatorAlternative) operator).getAlternatives().stream()
                    .map(OperatorAlternative.Alternative::getOperator)
                    .forEach(this::startTraversalFrom);

        } else if (operator.isSubplan()) {
            this.startTraversalFrom(((Subplan) operator).collectOutputOperators());

        } else if (operator.isExecutionOperator()) {
            final ExecutionOperator executionOperator = (ExecutionOperator) operator;
            final LoadProfile loadProfile = this.createLoadProfileFor(executionOperator);
            final TimeEstimate timeEstimate = this.getLoadProfileToTimeConverter().convert(loadProfile);
            this.timeEstimates.put(executionOperator, timeEstimate);
        }

    }

    /**
     * Create a new instance for an {@link ExecutionOperator} in the context of a plan.
     *
     * @param executionOperator    whose {@link LoadProfile} should be established
     * @return the new instance
     * @throws IllegalArgumentException if a required {@link CardinalityEstimate} is missing
     */
    private LoadProfile createLoadProfileFor(ExecutionOperator executionOperator)
            throws IllegalArgumentException {

        // Collect the assets necessary for the estimation.
        final LoadProfileEstimator estimator = getEstimatorFor(executionOperator);
        final CardinalityEstimate[] inputEstimates = collectInputEstimates(executionOperator, this.cardinalityEstimates);
        final CardinalityEstimate[] outputEstimates = collectOutputEstimates(executionOperator, this.cardinalityEstimates);

        // Perform the estimation.
        return estimator.estimate(inputEstimates, outputEstimates);
    }

    /**
     * Retrieve the {@link LoadProfileEstimator} for a given {@link ExecutionOperator}/
     */
    private LoadProfileEstimator getEstimatorFor(ExecutionOperator executionOperator) {
        final ConfigurationProvider<ExecutionOperator, LoadProfileEstimator> loadProfileEstimatorProvider =
                this.configuration.getOperatorLoadProfileEstimatorProvider();
        return loadProfileEstimatorProvider.provideFor(executionOperator);
    }

    /**
     * Assemble the input {@link CardinalityEstimate} array for a given {@link ExecutionOperator} from a precalculated
     * {@link CardinalityEstimate}s.
     */
    private static CardinalityEstimate[] collectInputEstimates(
            ExecutionOperator executionOperator,
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

    /**
     * Assemble the output {@link CardinalityEstimate} array for a given {@link ExecutionOperator} from a precalculated
     * {@link CardinalityEstimate}s.
     */
    private static CardinalityEstimate[] collectOutputEstimates(
            ExecutionOperator executionOperator,
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

    private LoadProfileToTimeConverter getLoadProfileToTimeConverter() {
        return this.configuration.getLoadProfileToTimeConverterProvider().provide();
    }

}
