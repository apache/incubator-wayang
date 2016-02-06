package org.qcri.rheem.core.optimizer.cardinality;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.plan.InputSlot;
import org.qcri.rheem.core.plan.OutputSlot;
import org.qcri.rheem.core.plan.Subplan;

import java.util.*;
import java.util.stream.Collectors;

/**
 * {@link CardinalityEstimator} that subsumes a DAG of operators, each one providing a local {@link CardinalityEstimator}.
 */
public class CompositeCardinalityEstimator implements CardinalityEstimator {

    private final CardinalityEstimationTraversal traversal;

    /**
     * Inner {@link OutputSlot} whose {@link CardinalityEstimate} is the final result.
     */
    private final OutputSlot<?> returnOutputSlot;

    /**
     * Create an instance for the given {@link Subplan}.
     *
     * @return the instance if it could be created
     */
    public static Optional<CardinalityEstimator> createFor(Subplan subplan,
                                                           int outputIndex,
                                                           final Configuration configuration) {
        final OutputSlot<?> subplanOutput = subplan.getOutput(outputIndex);
        final OutputSlot<?> innerOutput = subplan.traceOutput(subplanOutput);
        if (innerOutput == null) {
            return Optional.of(
                    new DefaultCardinalityEstimator(1d, subplan.getNumInputs(), inputCards -> 0L));
        }

        final List<Collection<InputSlot<?>>> innerInputs = Arrays.stream(subplan.getAllInputs())
                .map(inputSlot -> (Collection<InputSlot<?>>) (Collection) subplan.followInput(inputSlot))
                .collect(Collectors.toList());
        final CardinalityEstimationTraversal traversal = CardinalityEstimationTraversal.createPullTraversal(
                innerInputs, innerOutput, configuration);
        if (traversal == null) {
            return Optional.empty();
        }


        return Optional.of(new CompositeCardinalityEstimator(traversal, innerOutput));
    }

    /**
     * Creates a new instance.
     */
    private CompositeCardinalityEstimator(CardinalityEstimationTraversal traversal,
                                          final OutputSlot<?> returnOutputSlot) {
        this.traversal = traversal;
        this.returnOutputSlot = returnOutputSlot;
    }

    @Override
    synchronized public CardinalityEstimate estimate(Configuration configuration, CardinalityEstimate... inputEstimates) {
        final Map<OutputSlot<?>, CardinalityEstimate> terminalEstimates = this.traversal.traverse(configuration, inputEstimates);
        return terminalEstimates.get(this.returnOutputSlot);
    }


}
