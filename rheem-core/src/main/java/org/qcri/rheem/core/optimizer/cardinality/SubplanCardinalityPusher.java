package org.qcri.rheem.core.optimizer.cardinality;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.CompositeOperator;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.plan.rheemplan.LoopSubplan;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.OperatorContainer;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.plan.rheemplan.Subplan;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * {@link CardinalityPusher} implementation for {@link Subplan}s (but not for {@link LoopSubplan}s!)
 */
public class SubplanCardinalityPusher extends CardinalityPusher {

    private final CardinalityEstimationTraversal traversal;

    /**
     * Create an instance for the given {@link Subplan}.
     *
     * @return the instance if it could be created
     */
    public static CardinalityPusher createFor(OperatorContainer container, Configuration configuration) {
        final CompositeOperator compositeOperator = container.toOperator();
        final InputSlot<?>[] outerInputs = compositeOperator.getAllInputs();
        final List<InputSlot<?>> innerInputs = Arrays.stream(outerInputs)
                .flatMap(inputSlot -> container.followInput(inputSlot).stream())
                .collect(Collectors.toList());
        final Collection<Operator> sourceOperators = compositeOperator.isSource() ?
                Collections.singleton(container.getSource()) : Collections.emptySet();
        final CardinalityEstimationTraversal traversal = CardinalityEstimationTraversal.createPushTraversal(
                innerInputs, sourceOperators, configuration);

        return new SubplanCardinalityPusher(traversal, compositeOperator);
    }

    /**
     * Creates a new instance.
     */
    private SubplanCardinalityPusher(CardinalityEstimationTraversal traversal, final CompositeOperator compositeOperator) {
        super(compositeOperator);
        assert !compositeOperator.isLoopSubplan() : String.format("%s is not suited for %s instances.",
                this.getClass().getSimpleName(), Subplan.class.getSimpleName());
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
        Subplan subplan = (Subplan) opCtx.getOperator();
        for (int outputIndex = 0; outputIndex < subplan.getNumOutputs(); outputIndex++) {
            final OutputSlot<?> innerOutput = subplan.traceOutput(subplan.getOutput(outputIndex));
            if (innerOutput != null) {
                final OptimizationContext.OperatorContext innerOperatorCtx =
                        opCtx.getOptimizationContext().getOperatorContext(innerOutput.getOwner());
                final CardinalityEstimate cardinality = innerOperatorCtx.getOutputCardinality(innerOutput.getIndex());
                opCtx.setOutputCardinality(outputIndex, cardinality);
            }
        }
    }

}
