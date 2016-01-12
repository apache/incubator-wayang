package org.qcri.rheem.core.mapping;

import org.qcri.rheem.core.plan.*;

import java.util.*;

/**
 * A subplan pattern describes a class of subplans in a {@link org.qcri.rheem.core.plan.PhysicalPlan}.
 * <p><i>NB: Currently, only such patterns are tested and supported that form a chain of operators, i.e., no DAGs
 * are allowed and at most one input and one output operator.</i></p>
 */
public class SubplanPattern extends OperatorBase {

    private final OperatorPattern inputPattern, outputPattern;

    public SubplanPattern(OperatorPattern inputPattern, OperatorPattern outputPattern) {
        super(inputPattern.getAllInputs(), outputPattern.getAllOutputs(), null);
        this.inputPattern = inputPattern;
        this.outputPattern = outputPattern;
    }

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
        return new SubplanPattern(inputOperatorPattern, outputOperatorPattern);
    }

    public List<SubplanMatch> match(PhysicalPlan plan) {
        return new Matcher().match(plan);
    }

    public OperatorPattern getInputPattern() {
        return this.inputPattern;
    }

    public OperatorPattern getOutputPattern() {
        return this.outputPattern;
    }

    @Override
    public void accept(PlanVisitor visitor) {
        throw new RuntimeException("Pattern does not accept visitors.");
    }

    /**
     * This class encapsulates the functionality to match a subplan pattern against a plan.
     */
    private class Matcher {

        /**
         * Operators that have been considered to match against this pattern's sink.
         */
        final Set<Operator> visitedOutputOperators = new HashSet<>();

        /**
         * This stack keeps track of the currently visited {@link Subplan}s.
         */
        final Stack<Subplan> subplanStack = new Stack<>();

        final List<SubplanMatch> matches = new LinkedList<>();

        /**
         * Used to construct a match optimistically.
         */
        SubplanMatch currentSubplanMatch = null;

        public List<SubplanMatch> match(PhysicalPlan plan) {
            for (Operator sink : plan.getSinks()) {
                matchOutputPattern(sink, null);
            }
            return this.matches;
        }

        /**
         * Try to match the given operator pattern..
         *
         * @param operator the operator that should be matched with the operator pattern
         */
        private void matchOutputPattern(Operator operator, OutputSlot<?> fromSlot) {
            // We might run over the same operator twice in DAGs and trees coming from the sinks. Therefore, keep track
            // of the operators, we have visited so far.
            if (!this.visitedOutputOperators.add(operator)) {
                return;
            }

            if (operator instanceof Subplan) {
                final Subplan subplan = (Subplan) operator;
                if (fromSlot == null) {
                    matchOutputPattern(subplan.enter(), null);
                } else {
                    final OutputSlot<?> innerOutputSlot = subplan.enter(fromSlot);
                    if (innerOutputSlot != null) {
                        matchOutputPattern(innerOutputSlot.getOwner(), innerOutputSlot);
                    }
                }

            } else {

                // Try to make a match starting from the currently visited operator.
                this.currentSubplanMatch = new SubplanMatch(SubplanPattern.this);
                if (match(SubplanPattern.this.outputPattern, operator)) {
                    this.matches.add(this.currentSubplanMatch);
                }
                this.currentSubplanMatch = null;

                // Wander down the input plan and match.
                Arrays.stream(operator.getAllInputs())
                        .map(input -> {
                            final Operator parent = input.getOwner().getParent();
                            if (parent != null && parent instanceof Subplan) {
                                final InputSlot<?> outerInput = ((Subplan) parent).exit(input);
                                if (outerInput == null) return outerInput;
                            }
                            return input;
                        })
                        .map(InputSlot::getOccupant)
                        .filter(occupant -> occupant != null)
                        .forEach(occupant -> this.matchOutputPattern(occupant.getOwner(), occupant));
            }
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
