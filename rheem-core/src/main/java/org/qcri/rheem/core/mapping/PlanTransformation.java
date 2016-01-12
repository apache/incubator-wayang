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

    public int transform(PhysicalPlan plan) {
        int numTransformations = 0;
        List<SubplanMatch> matches;
        while (!(matches = pattern.match(plan)).isEmpty()) {
            final SubplanMatch match = matches.get(0);
            final Operator replacement = this.replacementFactory.createReplacementSubplan(match);
            replace(plan, match, replacement);
            numTransformations++;
        }

        return numTransformations;
    }

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
