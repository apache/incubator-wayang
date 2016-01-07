package org.qcri.rheem.core.mapping;

import org.qcri.rheem.core.plan.*;

import java.util.*;

/**
 * A subplan pattern describes a class of subplans in a {@link org.qcri.rheem.core.plan.PhysicalPlan}.
 * <p><i>NB: Currently, only such patterns are tested and supported that form a chain of operators, i.e., no DAGs
 * are allowed and at most one input and one output operator.</i></p>
 */
public class SubplanPattern implements Operator {

    private OperatorPattern inputPattern, outputPattern;

    /**
     * Creates a new instance that matches only a single operator.
     *
     * @param operatorPattern the only operator pattern
     * @return the new instance
     */
    public static final SubplanPattern createSingleton(OperatorPattern operatorPattern) {
        return fromOperatorPatterns(operatorPattern, operatorPattern);
    }

    /**
     * Creates a new instance that matches a graph of operator patterns.
     *
     * @param inputOperatorPattern  the only operator pattern that has inputs wrt. the subplan pattern
     * @param outputOperatorPattern the only operator pattern that has outputs wrt. the subplan pattern
     * @return the new instance
     */
    public static final SubplanPattern fromOperatorPatterns(OperatorPattern inputOperatorPattern,
                                                            OperatorPattern outputOperatorPattern) {
        final SubplanPattern subplanPattern = new SubplanPattern();
        subplanPattern.inputPattern = inputOperatorPattern;
        subplanPattern.outputPattern = outputOperatorPattern;
        return subplanPattern;
    }

    @Override
    public InputSlot[] getAllInputs() {
        return this.inputPattern.getAllInputs();
    }

    @Override
    public OutputSlot[] getAllOutputs() {
        return this.outputPattern.getAllOutputs();
    }

    public List<SubplanMatch> match(PhysicalPlan plan) {
        return new Matcher().match(plan);
    }

    public OperatorPattern getInputPattern() {
        return inputPattern;
    }

    public OperatorPattern getOutputPattern() {
        return outputPattern;
    }

    /**
     * This class encapsulates the functionality to match a subplan pattern against a plan.
     */
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
         *
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
            if (match(SubplanPattern.this.outputPattern, operator)) {
                this.matches.add(this.currentSubplanMatch);
            }
            this.currentSubplanMatch = null;

            // Wander down the input plan and match.
            Arrays.stream(operator.getAllInputs())
                    .map(input -> input.getOccupant())
                    .filter(occupant -> occupant != null)
                    .forEach(occupant -> this.matchOutputPattern(occupant.getOwner()));
        }

        /**
         * Recursively match the given operator pattern and operator including their input operators, thereby building
         * recording the matches in {@link #currentSubplanMatch}.
         *
         * @return whether the recursive match was successful
         */
        private boolean match(OperatorPattern pattern, Operator operator) {
            final OperatorMatch operatorMatch = pattern.match(operator);
            if (operatorMatch != null) {
                this.currentSubplanMatch.addOperatorMatch(operatorMatch);

                // Now we need to try to match all the input operator patterns.
                for (int inputIndex = 0; inputIndex < operator.getNumInputs(); inputIndex++) {
                    final OperatorPattern inputOperatorPattern = (OperatorPattern) pattern.getInputOperator(inputIndex);
                    if (inputOperatorPattern != null) {
                        final Operator inputOperator = operator.getInputOperator(inputIndex);
                        if (!match(inputOperatorPattern, inputOperator)) {
                            return false;
                        }
                    }
                }
                return true;
            }

            return false;
        }
    }
}
