package org.qcri.rheem.core.plan.rheemplan;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.util.OneTimeExecutable;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

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
        if (this.rheemPlan.isLoopsIsolated()) return;
        @SuppressWarnings("unchecked")
        final Collection<LoopHeadOperator> loopHeads =
                (Collection<LoopHeadOperator>) (Collection) PlanTraversal.upstream()
                        .traverse(this.rheemPlan.getSinks())
                        .getTraversedNodesWith(Operator::isLoopHead);

        loopHeads.forEach(LoopIsolator::isolate);
        this.rheemPlan.setLoopsIsolated();
    }

    /**
     * Isolates the loop starting at the {@code allegedLoopHead}.
     *
     * @return the isolated {@link LoopSubplan} or {@code null} if none could be isolated
     */
    public static LoopSubplan isolate(Operator allegedLoopHead) {
        if (!allegedLoopHead.isLoopHead()) return null;
        LoopHeadOperator loopHead = (LoopHeadOperator) allegedLoopHead;

        // Collect the InputSlots of the loop.
        final Collection<InputSlot<?>> bodyInputSlots = collectInboundInputs(loopHead);
        List<InputSlot<?>> loopInputSlots = new ArrayList<>(loopHead.getLoopInitializationInputs().size() + bodyInputSlots.size());
        loopInputSlots.addAll(loopHead.getLoopInitializationInputs());
        loopInputSlots.addAll(bodyInputSlots);
        List<OutputSlot<?>> loopOutputSlots = new ArrayList<>(loopHead.getFinalLoopOutputs());

        // TODO In the best case, we should also do the following sanity check: Is any loop body operator reachable...
        // ...from the loop head's final OutputSlots? That would be fatal.

        // Insert a new Subplan to delimit the loop body.
        return LoopSubplan.wrap(loopHead, loopInputSlots, loopOutputSlots);
    }

    /**
     * Collect all {@link InputSlot}s in the loop body corresponding to the {@code loopHead} that are not fed by the
     * loop itself. Also, checks various sanity aspects of the loop body.
     */
    private static Collection<InputSlot<?>> collectInboundInputs(LoopHeadOperator loopHead) {
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
            Validate.notNull(inputSlot.getOccupant(), "Loop body input %s is unconnected.", inputSlot);
            Validate.isTrue(loopBodyOperators.contains(inputSlot.getOccupant().getOwner()),
                    "Illegal input for loop head input %s.", inputSlot);
        }
        for (InputSlot<?> initializationInput : loopHead.getLoopInitializationInputs()) {
            if (initializationInput.getOccupant() == null) continue;
            Validate.isTrue(!loopBodyOperators.contains(initializationInput.getOccupant().getOwner()),
                    "Illegal input for loop head input %s.", initializationInput);
        }

        // Sanity-check the outputs of the loopHead.
        for (OutputSlot<?> outputSlot : loopHead.getLoopBodyOutputs()) {
            final List<? extends InputSlot<?>> occupiedSlots = outputSlot.getOccupiedSlots();
            if (occupiedSlots.isEmpty()) {
                LoggerFactory.getLogger(LoopIsolator.class).warn("{} is not feeding any input slot.", outputSlot);
            }
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
