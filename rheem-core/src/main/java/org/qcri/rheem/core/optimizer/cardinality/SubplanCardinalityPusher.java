package org.qcri.rheem.core.optimizer.cardinality;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.*;

import java.util.*;
import java.util.stream.Collectors;

/**
 * {@link CardinalityPusher} implementation for {@link Subplan}s (but not for {@link LoopSubplan}s!)
 */
public class SubplanCardinalityPusher extends CardinalityPusher {

    private final CardinalityEstimationTraversal traversal;

    private final Subplan subplan;

    /**
     * Create an instance for the given {@link Subplan}.
     *
     * @return the instance if it could be created
     */
    public static CardinalityPusher createFor(Subplan subplan, Configuration configuration) {

        final List<InputSlot<?>> innerInputs = Arrays.stream(subplan.getAllInputs())
                .flatMap(inputSlot -> subplan.followInput(inputSlot).stream())
                .collect(Collectors.toList());
        final Collection<Operator> sourceOperators = subplan.isSource() ?
                Collections.singleton(subplan.getSource()) : Collections.emptySet();
        final CardinalityEstimationTraversal traversal = CardinalityEstimationTraversal.createPushTraversal(
                innerInputs, sourceOperators, configuration);

        return new SubplanCardinalityPusher(traversal, subplan);
    }

    /**
     * Creates a new instance.
     */
    private SubplanCardinalityPusher(CardinalityEstimationTraversal traversal, final Subplan subplan) {
        assert !subplan.isLoopSubplan();
        this.subplan = subplan;
        this.traversal = traversal;
    }

    @Override
    protected boolean canUpdate(OptimizationContext.OperatorContext opCtx) {
        // We always try to update because there might be internal updates.
        return true;
    }

    @Override
    protected void doPush(OptimizationContext.OperatorContext opCtx, Configuration configuration) {
        // Kick the traversal off.
        this.traversal.traverse(opCtx.getOptimizationContext(), configuration);

        // Pull the cardinalities for the OutputSlots.
        for (int outputIndex = 0; outputIndex < this.subplan.getNumOutputs(); outputIndex++) {
            final OutputSlot<?> innerOutput = this.subplan.traceOutput(this.subplan.getOutput(outputIndex));
            if (innerOutput != null) {
                final OptimizationContext.OperatorContext innerOperatorCtx =
                        opCtx.getOptimizationContext().getOperatorContext(innerOutput.getOwner());
                final CardinalityEstimate cardinality = innerOperatorCtx.getOutputCardinality(innerOutput.getIndex());
                opCtx.setOutputCardinality(outputIndex, cardinality);
            }
        }
    }

}
