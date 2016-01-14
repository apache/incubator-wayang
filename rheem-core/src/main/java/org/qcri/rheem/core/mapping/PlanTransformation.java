package org.qcri.rheem.core.mapping;

import org.qcri.rheem.core.plan.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A plan transformation looks for a plan pattern in a {@link PhysicalPlan} and replaces it
 * with an alternative subplan.
 */
public class PlanTransformation {

    private final SubplanPattern pattern;

    private final ReplacementSubplanFactory replacementFactory;

    public PlanTransformation(SubplanPattern pattern, ReplacementSubplanFactory replacementFactory) {
        this.pattern = pattern;
        this.replacementFactory = replacementFactory;
    }

    /**
     * Apply this transformation exhaustively on the current plan.
     *
     * @param plan  the plan to which the transformation should be applied
     * @param epoch (i) the epoch for new plan parts and (ii) match only operators whose epoch is less than {@code epoch - 1}
     * @return the number of applied transformations
     * @see Operator#getEpoch()
     */
    public int transform(PhysicalPlan plan, int epoch) {
        int numTransformations = 0;
        List<SubplanMatch> matches;
        while (!(matches = pattern.match(plan, epoch - 1)).isEmpty()) {
            final SubplanMatch match = matches.get(0);
            final Operator replacement = this.replacementFactory.createReplacementSubplan(match, epoch);
            introduceAlternative(plan, match, replacement);
            numTransformations++;
        }

        return numTransformations;
    }

    private void introduceAlternative(PhysicalPlan plan, SubplanMatch match, Operator replacement) {

        // Wrap the match in a subplan.
        final Operator originalOutputOperator = match.getOutputMatch().getOperator();
        Operator originalSubplan = Subplan.wrap(match.getInputMatch().getOperator(), originalOutputOperator);

        // Place an alternative: the original subplan and the replacement.
        // TODO: ensure flat alternatives
        // TODO: keep track of provenance of alternatives
        OperatorAlternative operatorAlternative = OperatorAlternative.wrap(originalSubplan);
        operatorAlternative.addAlternative(replacement);

        // If the originalOutputOperator was a sink, we need to update the sink in the plan accordingly.
        if (originalOutputOperator.isSink()) {
            plan.getSinks().remove(originalOutputOperator);
            plan.addSink(operatorAlternative);
        }
    }

    /**
     * @deprecated use {@link #introduceAlternative(PhysicalPlan, SubplanMatch, Operator)}
     */
    private void replace(PhysicalPlan plan, SubplanMatch match, Operator replacement) {
        // Disconnect the original input operator and insert the replacement input operator.
        final Operator originalInputOperator = match.getInputMatch().getOperator();
        for (int inputIndex = 0; inputIndex < originalInputOperator.getNumInputs(); inputIndex++) {
            final InputSlot originalInputSlot = originalInputOperator.getInput(inputIndex);
            final OutputSlot occupant = originalInputSlot.getOccupant();
            if (occupant != null) {
                occupant.disconnectFrom(originalInputSlot);
                occupant.connectTo(replacement.getInput(inputIndex));
            }
        }

        // Disconnect the original output operator and insert the replacement output operator.
        final Operator originalOutputOperator = match.getOutputMatch().getOperator();
        for (int outputIndex = 0; outputIndex < originalOutputOperator.getNumOutputs(); outputIndex++) {
            final OutputSlot originalOutputSlot = originalOutputOperator.getOutput(outputIndex);
            for (InputSlot inputSlot : new ArrayList<InputSlot>(originalOutputSlot.getOccupiedSlots())) {
                originalOutputSlot.disconnectFrom(inputSlot);
                replacement.getOutput(outputIndex).connectTo(inputSlot);
            }
        }

        // If the originalOutputOperator was a sink, we need to update the sink in the plan accordingly.
        if (originalOutputOperator.isSink()) {
            plan.getSinks().remove(originalOutputOperator);
            final Collection<Operator> sinks = new PlanTraversal(true, true)
                    .traverse(replacement)
                    .getTraversedNodesWith(Operator::isSink);
            for (Operator sink : sinks) {
                plan.addSink(sink);
            }
        }
    }
}
