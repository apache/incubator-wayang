package org.qcri.rheem.core.plan.rheemplan;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.util.OneTimeExecutable;

import java.util.*;

/**
 * Goes over a {@link RheemPlan} and isolates its loops.
 */
public class LoopIsolator extends OneTimeExecutable {

    // Refactor: We don't need the OneTimeExecutable here (as of now).

    private final RheemPlan rheemPlan;

    private LoopIsolator(RheemPlan rheemPlan) {
        this.rheemPlan = rheemPlan;
    }

    public static void isolateLoops(RheemPlan rheemPlan) {
        new LoopIsolator(rheemPlan).run();
    }

    private void run() {
        this.execute();
    }

    @Override
    protected void doExecute() {
        @SuppressWarnings("unchecked")
        final Collection<LoopHeadOperator> loopHeads =
                (Collection<LoopHeadOperator>) (Collection) PlanTraversal.upstream()
                        .traverse(this.rheemPlan.getSinks())
                        .getTraversedNodesWith(Operator::isLoopHead);

        loopHeads.forEach(this::isolate);
    }

    private void isolate(Operator allegedLoopHead) {
        if (!allegedLoopHead.isLoopHead()) return;
        LoopHeadOperator loopHead = (LoopHeadOperator) allegedLoopHead;

        // Collect the InputSlots of the loop.
        final Collection<InputSlot<?>> bodyInputSlots = this.collectInboundInputs(loopHead);
        List<InputSlot<?>> loopInputSlots = new ArrayList<>(loopHead.getLoopInitializationInputs().size() + bodyInputSlots.size());
        loopInputSlots.addAll(loopHead.getLoopInitializationInputs());
        loopInputSlots.addAll(bodyInputSlots);
        List<OutputSlot<?>> loopOutputSlots = new ArrayList<>(loopHead.getFinalLoopOutputs());

        // TODO In the best case, we should also do the following sanity check: Is any loop body operator reachable...
        // ...from the loop head's final OutputSlots? That would be fatal.

        // Insert a new Subplan to delimit the loop body.
        LoopSubplan.wrap(loopHead, loopInputSlots, loopOutputSlots, loopHead.getContainer());
    }

    /**
     * Collect all {@link InputSlot}s in the loop body corresponding to the {@code loopHead} that are not fed by the
     * loop itself. Also, checks various sanity aspects of the loop body.
     */
    private Collection<InputSlot<?>> collectInboundInputs(LoopHeadOperator loopHead) {
        // Gather all Operators that are part of the loop.
        final Collection<Operator> loopBodyOperators = PlanTraversal.downstream()
                .traverseFocused(loopHead, loopHead.getLoopBodyOutputs())
                .getTraversedNodes();

        // Collect inbound InputSlots and perform sanity checks.
        Collection<InputSlot<?>> inboundInputSlots = new HashSet<>();
        for (Operator loopBodyOperator : loopBodyOperators) {
            if (loopBodyOperator == loopHead) continue;

            // Find such InputSlots that are fed from outside of this loop.
            final InputSlot<?>[] inputs = loopBodyOperator.getAllInputs();
            for (InputSlot<?> input : inputs) {
                final OutputSlot<?> occupant = input.getOccupant();
                if (occupant != null && !loopBodyOperators.contains(occupant.getOwner())) {
                    inboundInputSlots.add(input);
                }
            }

            // Sanity check the OutputSlots.
            Validate.isTrue(!loopBodyOperator.isSink(), "Disallowed sink %s in loop body of %s.", loopBodyOperator, loopHead);
        }

        // Sanity-check inputs of the loopHead.
        for (InputSlot<?> inputSlot : loopHead.getLoopBodyInputs()) {
            Validate.notNull(inputSlot.getOccupant(), "%s is unconnected.", inputSlot);
            Validate.isTrue(loopBodyOperators.contains(inputSlot.getOccupant().getOwner()),
                    "Illegal input for loop head input %s.", inputSlot);
        }
        for (InputSlot<?> inputSlot : loopHead.getLoopInitializationInputs()) {
            Validate.notNull(inputSlot.getOccupant(), "%s is unconnected.", inputSlot);
            Validate.isTrue(!loopBodyOperators.contains(inputSlot.getOccupant().getOwner()),
                    "Illegal input for loop head input %s.", inputSlot);
        }

        // Sanity-check the outputs of the loopHead.
        for (OutputSlot<?> outputSlot : loopHead.getLoopBodyOutputs()) {
            final List<? extends InputSlot<?>> occupiedSlots = outputSlot.getOccupiedSlots();
            Validate.isTrue(!occupiedSlots.isEmpty());
        }
        for (OutputSlot<?> outputSlot : loopHead.getFinalLoopOutputs()) {
            final List<? extends InputSlot<?>> occupiedSlots = outputSlot.getOccupiedSlots();
            for (InputSlot<?> occupiedSlot : occupiedSlots) {
                Validate.isTrue(!loopBodyOperators.contains(occupiedSlot.getOwner()),
                        "%s is inside and outside the loop body of %s.", occupiedSlot.getOccupant(), loopHead);
            }
        }

        return inboundInputSlots;
    }
}
