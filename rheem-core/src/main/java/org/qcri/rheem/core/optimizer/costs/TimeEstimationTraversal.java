package org.qcri.rheem.core.optimizer.costs;

import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.plan.*;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Traverses a {@link PhysicalPlan}, thereby creating {@link TimeEstimate}s for every encountered operator.
 */
public class TimeEstimationTraversal {

    private final ResourceUsageProfileToTimeConverter converter;

    private final Map<OutputSlot<?>, CardinalityEstimate> cardinalityEstimates;

    private PlanTraversal planTraversal = new PlanTraversal(true, false)
            .withCallback((operator, fromInputSlot, fromOutputSlot) -> this.handleOperator(operator));

    private final Map<ExecutionOperator, TimeEstimate> timeEstimates = new HashMap<>();

    private TimeEstimationTraversal(ResourceUsageProfileToTimeConverter converter,
                                    Map<OutputSlot<?>, CardinalityEstimate> cardinalityEstimates) {
        this.converter = converter;
        this.cardinalityEstimates = cardinalityEstimates;
    }

    /**
     * Traverse the given plan and its subplans, thereby determining {@link TimeEstimate}s for all encountered
     * {@link ExecutionOperator}s.
     *
     * @param plan                 should be traversed
     * @param converter            converts {@link ResourceUsageProfile}s of {@link ExecutionOperator}s to {@link TimeEstimate}s
     * @param cardinalityEstimates provides {@link CardinalityEstimate}s of the {@link PhysicalPlan}
     * @return the {@link TimeEstimate}s for all encountered {@link ExecutionOperator}s
     */
    public static Map<ExecutionOperator, TimeEstimate> traverse(
            PhysicalPlan plan,
            ResourceUsageProfileToTimeConverter converter,
            Map<OutputSlot<?>, CardinalityEstimate> cardinalityEstimates) {

        TimeEstimationTraversal instance = new TimeEstimationTraversal(converter, cardinalityEstimates);
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
            final ResourceUsageProfile resourceUsageProfile =
                    ResourceUsageProfile.createFor(executionOperator, this.cardinalityEstimates);
            final TimeEstimate timeEstimate = this.converter.convert(resourceUsageProfile);
            this.timeEstimates.put(executionOperator, timeEstimate);
        }

    }

}
