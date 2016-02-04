package org.qcri.rheem.core.optimizer.cardinality;

import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.plan.*;

import java.util.*;
import java.util.stream.Collectors;

public class CompositeCardinalityPusher extends CardinalityPusher {

    private final CardinalityEstimationTraversal traversal;

    /**
     * Create an instance for the given {@link Subplan}.
     *
     * @return the instance if it could be created
     */
    public static CardinalityPusher createFor(Subplan subplan, Map<OutputSlot<?>, CardinalityEstimate> cache) {

        final List<Collection<InputSlot<?>>> innerInputs = Arrays.stream(subplan.getAllInputs())
                .map(inputSlot -> (Collection<InputSlot<?>>) (Collection) subplan.followInput(inputSlot))
                .collect(Collectors.toList());
        final CardinalityEstimationTraversal traversal = CardinalityEstimationTraversal.createPushTraversal(
                innerInputs, Collections.emptyList(), cache);

        return new CompositeCardinalityPusher(traversal, subplan, cache);
    }

    /**
     * Create an instance for the given {@link PhysicalPlan}.
     *
     * @return the instance if it could be created
     */
    public static CardinalityPusher createFor(PhysicalPlan physicalPlan, Map<OutputSlot<?>, CardinalityEstimate> cache) {

        final Collection<Operator> sources = physicalPlan.collectReachableTopLevelSources();
        final CardinalityEstimationTraversal traversal = CardinalityEstimationTraversal.createPushTraversal(
                Collections.emptyList(), sources, cache);

        return new CompositeCardinalityPusher(traversal, Operators.slotlessOperator(), cache);
    }

    /**
     * Creates a new instance.
     */
    private CompositeCardinalityPusher(CardinalityEstimationTraversal traversal,
                                       final Operator operator,
                                       final Map<OutputSlot<?>, CardinalityEstimate> cache) {
        super(operator, cache);
        this.traversal = traversal;
    }

    @Override
    protected CardinalityEstimate[] doPush(RheemContext rheemContext, CardinalityEstimate... inputEstimates) {
        final Map<OutputSlot<?>, CardinalityEstimate> terminalEstimates =
                this.traversal.traverse(rheemContext, inputEstimates);
        return constructPushResult(terminalEstimates);
    }

    /**
     * Collect the relevant {@link CardinalityEstimate}s from the {@code terminalEstimates} and pack them into
     * an array as expected by {@link CardinalityPusher#push(RheemContext, CardinalityEstimate...)}.
     */
    private CardinalityEstimate[] constructPushResult(Map<OutputSlot<?>, CardinalityEstimate> terminalEstimates) {
        final CardinalityEstimate[] result = new CardinalityEstimate[this.getOperator().getNumOutputs()];
        for (int outputIndex = 0; outputIndex < result.length; outputIndex++) {
            OutputSlot<?> outerOutput = this.getOperator().getOutput(outputIndex);
            OutputSlot<?> innerOutput = ((Subplan) this.getOperator()).traceOutput(outerOutput);
            if (innerOutput == null) {
                continue;
            }
            final CardinalityEstimate cardinalityEstimate = terminalEstimates.get(innerOutput);
            result[outputIndex] = cardinalityEstimate;
        }
        return result;

    }
}
