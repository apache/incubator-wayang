package org.qcri.rheem.core.mapping;

import org.qcri.rheem.core.plan.*;

import java.util.*;
import java.util.function.Predicate;

/**
 * A subplan pattern describes a class of subplans in a {@link org.qcri.rheem.core.plan.PhysicalPlan}.
 */
public class SubplanPattern implements Operator {

    private OperatorPattern inputOperator, outputOperator;

    public static final SubplanPattern createSingleton(OperatorPattern operatorPattern) {
        final SubplanPattern subplanPattern = new SubplanPattern();
        subplanPattern.inputOperator = operatorPattern;
        subplanPattern.outputOperator = operatorPattern;
        return subplanPattern;
    }

    @Override
    public InputSlot[] getAllInputs() {
        return this.inputOperator.getAllInputs();
    }

    @Override
    public OutputSlot[] getAllOutputs() {
        return this.outputOperator.getAllOutputs();
    }

    public List<SubplanMatch> match(PhysicalPlan plan) {
        return new Matcher().match(plan);
    }

    private class Matcher {

        /**
         * Operators that have been considered to match against this pattern's sink.
         */
        final Set<Operator> visitedOutputOperators = new HashSet<>();

        final List<SubplanMatch> matches = new LinkedList<>();

        /**
         * Used to construct a match optimistically.
         */
        SubplanMatch currentSubplanMatch = null;

        public List<SubplanMatch> match(PhysicalPlan plan) {
            for (Sink sink : plan.getSinks()) {
                matchOutputPattern(sink);
            }
            return this.matches;
        }

        /**
         * Try to match the given operator pattern..
         * @param operator the operator that should be matched with the operator pattern
         */
        private void matchOutputPattern(Operator operator) {
            // We might run over the same operator twice in DAGs and trees coming from the sinks. Therefore, keep track
            // of the operators, we have visited so far.
            if (!this.visitedOutputOperators.add(operator)) {
                return;
            }

            // Try to make a match starting from the currently visited operator.
            this.currentSubplanMatch = new SubplanMatch(SubplanPattern.this);
            if (match(SubplanPattern.this.outputOperator, operator)) {
                this.matches.add(this.currentSubplanMatch);
            }
            this.currentSubplanMatch = null;

            // Wander down the input plan and match.
            Arrays.stream(operator.getAllInputs())
                    .map(input -> input.getOccupant())
                    .filter(occupant -> occupant == null)
                    .forEach(occupant -> this.matchOutputPattern(occupant.getOwner()));
        }

        private boolean match(OperatorPattern pattern, Operator operator) {
            final OperatorMatch operatorMatch = pattern.match(operator);
            if (operatorMatch != null) {
                this.currentSubplanMatch.addOperatorMatch(operatorMatch);

                // Now we need to try to match all the input operator patterns.
                for (int inputIndex = 0; inputIndex < operator.getNumInputs(); inputIndex++) {
                    final OutputSlot patternInput = pattern.getInput(inputIndex).getOccupant();
                    if (patternInput != null) {
                        final InputSlot operatorInput = operator.getInput(inputIndex);
                        if (!match((OperatorPattern) patternInput.getOwner(), operatorInput.getOwner())) {
                            return false;
                        }
                    }
                }
            }

            return true;
        }
    }
}
