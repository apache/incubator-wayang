package org.qcri.rheem.core.optimizer.cardinality;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.plan.rheemplan.LoopHeadOperator;
import org.qcri.rheem.core.plan.rheemplan.LoopSubplan;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;

import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * {@link CardinalityPusher} implementation for {@link LoopSubplan}s.
 */
public class LoopSubplanCardinalityPusher extends CardinalityPusher {

    private final CardinalityPusher loopHeadIterationPusher;

    private final CardinalityEstimationTraversal bodyTraversal;

    private final CardinalityPusher loopHeadInitializationPusher;

    private final CardinalityPusher loopHeadFinalizationPusher;

    private final Set<OutputSlot<?>> bodyOutputSlots;

    public LoopSubplanCardinalityPusher(LoopSubplan loopSubplan, Configuration configuration) {
        super(loopSubplan);

        // Create the CardinalityPusher for the loop head.
        final LoopHeadOperator loopHead = loopSubplan.getLoopHead();
        this.loopHeadInitializationPusher = loopHead.getInitializationPusher(configuration);
        this.loopHeadIterationPusher = loopHead.getCardinalityPusher(configuration);
        this.loopHeadFinalizationPusher = loopHead.getFinalizationPusher(configuration);

        // Create the CardinalityTraversal for the loop body.
        Set<InputSlot<?>> bodyInputSlots = Arrays.stream(loopSubplan.getAllInputs())
                .flatMap(outerInput -> loopSubplan.followInput(outerInput).stream())
                .collect(Collectors.toSet());
        for (InputSlot<?> inputSlot : loopHead.getLoopInitializationInputs()) {
            bodyInputSlots.remove(inputSlot);
        }
        for (OutputSlot<?> outputSlot : loopHead.getLoopBodyOutputs()) {
            for (InputSlot<?> inputSlot : outputSlot.getOccupiedSlots()) {
                bodyInputSlots.add(inputSlot);
            }
        }
        this.bodyOutputSlots = loopHead.getLoopBodyInputs().stream()
                .map(InputSlot::getOccupant)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
        this.bodyTraversal = CardinalityEstimationTraversal.createPushTraversal(
                bodyInputSlots, loopHead.getLoopBodyInputs(), Collections.emptyList(), configuration);
    }

    @Override
    protected void doPush(OptimizationContext.OperatorContext opCtx, Configuration configuration) {
        final OptimizationContext optimizationCtx = opCtx.getOptimizationContext();
        final LoopSubplan loopSubplan = (LoopSubplan) opCtx.getOperator();
        final OptimizationContext.LoopContext loopCtx = optimizationCtx.getNestedLoopContext(loopSubplan);
        final LoopHeadOperator loopHead = loopSubplan.getLoopHead();

        for (OptimizationContext iterationCtx : loopCtx.getIterationContexts()) {
            // Push through the loop head.
            final OptimizationContext.OperatorContext loopHeadCtx = iterationCtx.getOperatorContext(loopHead);
            if (iterationCtx.isFinalIteration()) {
                this.loopHeadFinalizationPusher.push(loopHeadCtx, configuration);

                // Pull the cardinalities for the OutputSlots.
                for (int outputIndex = 0; outputIndex < loopSubplan.getNumOutputs(); outputIndex++) {
                    final OutputSlot<?> innerOutput = loopSubplan.traceOutput(loopSubplan.getOutput(outputIndex));
                    if (innerOutput != null) {
                        final CardinalityEstimate cardinality = loopHeadCtx.getOutputCardinality(innerOutput.getIndex());
                        opCtx.setOutputCardinality(outputIndex, cardinality);
                    }
                }

                continue; // That's it for the final iteration.

            } else if (iterationCtx.isInitialIteration()) {
                this.loopHeadInitializationPusher.push(loopHeadCtx, configuration);

            } else {
                this.loopHeadIterationPusher.push(loopHeadCtx, configuration);
            }
            for (OutputSlot<?> outputSlot : loopHead.getLoopBodyOutputs()) {
                loopHeadCtx.pushCardinalityForward(outputSlot.getIndex(), iterationCtx);
            }

            // Push through the loop body.
            this.bodyTraversal.traverse(iterationCtx, configuration);
            for (OutputSlot<?> bodyOutputSlot : this.bodyOutputSlots) {
                final Operator bodyOperator = bodyOutputSlot.getOwner();
                final OptimizationContext.OperatorContext bodyOperatorCtx = iterationCtx.getOperatorContext(bodyOperator);
                bodyOperatorCtx.pushCardinalityForward(bodyOutputSlot.getIndex(), iterationCtx.getNextIterationContext());
            }
        }

    }

}
