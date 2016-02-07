package org.qcri.rheem.core.mapping;

import org.qcri.rheem.core.plan.*;
import org.qcri.rheem.core.platform.Platform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * A plan transformation looks for a plan pattern in a {@link PhysicalPlan} and replaces it
 * with an alternative subplan.
 */
public class PlanTransformation {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final SubplanPattern pattern;

    private boolean isReplacing = false;

    private final ReplacementSubplanFactory replacementFactory;

    private final Collection<Platform> targetPlatforms;

    public PlanTransformation(SubplanPattern pattern,
                              ReplacementSubplanFactory replacementFactory,
                              Platform... targetPlatforms) {
        this.pattern = pattern;
        this.replacementFactory = replacementFactory;
        this.targetPlatforms = Arrays.asList(targetPlatforms);
    }

    /**
     * Make this instance replace on matches instead of introducing alternatives.
     *
     * @return this instance
     */
    public PlanTransformation thatReplaces() {
        this.isReplacing = true;
        return this;
    }

    /**
     * Apply this transformation exhaustively on the current plan.
     *
     * @param plan  the plan to which the transformation should be applied
     * @param epoch (i) the epoch for new plan parts and (ii) match only operators whose epoch is equal to or
     *              greater than {@code epoch-1}
     * @return the number of applied transformations
     * @see Operator#getEpoch()
     */
    public int transform(PhysicalPlan plan, int epoch) {
        int numTransformations = 0;
        List<SubplanMatch> matches = pattern.match(plan, epoch - 1);
        for (SubplanMatch match : matches) {
            final Operator replacement = this.replacementFactory.createReplacementSubplan(match, epoch);

            if (match.getInputMatch() == match.getOutputMatch()) {
                logger.debug("Replacing {} with {} in epoch {}.",
                        match.getOutputMatch().getOperator(),
                        replacement,
                        epoch);
            } else {
                logger.debug("Replacing {}..{} with {} in epoch {}.",
                        match.getInputMatch().getOperator(),
                        match.getOutputMatch().getOperator(),
                        replacement,
                        epoch);
            }

            if (!this.meetsPlatformRestrictions(match)) {
                continue;
            }

            if (this.isReplacing) {
                replace(plan, match, replacement);
            } else {
                introduceAlternative(plan, match, replacement);
            }
            numTransformations++;
        }

        return numTransformations;
    }

    /**
     * Check if this instances does not violate any of the {@link Operator#getTargetPlatforms()} restrictions.
     */
    private boolean meetsPlatformRestrictions(SubplanMatch match) {
        // Short-cut: This transformation is not introducing some platform dependency.
        if (this.getTargetPlatforms().isEmpty()) {
            return true;
        }

        // Short-cut: The matched operators do not require specific platforms.
        if (!match.getTargetPlatforms().isPresent()) {
            return true;
        }
        // Otherwise check if
        return match.getTargetPlatforms().get().containsAll(this.getTargetPlatforms());
    }

    private void introduceAlternative(PhysicalPlan plan, SubplanMatch match, Operator replacement) {

        // Wrap the match in a subplan.
        final Operator originalOutputOperator = match.getOutputMatch().getOperator();
        Operator originalSubplan = Subplan.wrap(match.getInputMatch().getOperator(), originalOutputOperator);

        // Place an alternative: the original subplan and the replacement.
        // Either add an alternative to the existing OperatorAlternative or create a new OperatorAlternative.
        final CompositeOperator originalParent = originalSubplan.getParent();
        if (originalParent != null && originalParent instanceof OperatorAlternative) {
            ((OperatorAlternative) originalParent).addAlternative(replacement);

        } else {
            OperatorAlternative operatorAlternative = OperatorAlternative.wrap(originalSubplan);
            operatorAlternative.addAlternative(replacement);

            // If the originalOutputOperator was a sink, we need to update the sink in the plan accordingly.
            if (originalOutputOperator.isSink()) {
                plan.getSinks().remove(originalOutputOperator);
            }
            if (operatorAlternative.isSink() && operatorAlternative.getParent() == null) {
                plan.addSink(operatorAlternative);
            }
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

    public Collection<Platform> getTargetPlatforms() {
        return this.targetPlatforms;
    }
}
