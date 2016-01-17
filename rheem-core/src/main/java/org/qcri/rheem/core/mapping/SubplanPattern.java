package org.qcri.rheem.core.mapping;

import org.qcri.rheem.core.plan.*;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

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

    /**
     * Match this pattern against a plan.
     *
     * @param plan     the plan to match against
     * @param minEpoch the (inclusive) minimum epoch value for matched subplans
     * @return all matches
     */
    public List<SubplanMatch> match(PhysicalPlan plan, int minEpoch) {
        return new Matcher(minEpoch).match(plan);
    }

    public OperatorPattern getInputPattern() {
        return this.inputPattern;
    }

    public OperatorPattern getOutputPattern() {
        return this.outputPattern;
    }

    @Override
    public <Payload, Return> Return accept(PlanVisitor<Payload, Return> visitor, OutputSlot<?> outputSlot, Payload payload) {
        throw new RuntimeException("Pattern does not accept visitors.");
    }

    /**
     * This class encapsulates the functionality to match a subplan pattern against a plan. It works as follows:
     * <ol>
     * <li>for each operator in the plan, start a new match attempt</li>
     * <li>iterate through the graph pattern</li>
     * <li>co-iterate the actual graph</li>
     * <li>if we could co-iterate the two, create a match for the match attempt</li>
     * </ol>
     */
    private class Matcher {

        /**
         * Operators that have been considered to match against this pattern's sink.
         */
        final private Set<Operator> visitedOutputOperators = new HashSet<>();

        /**
         * Collects all the matches.
         */
        final private List<SubplanMatch> matches = new LinkedList<>();

        /**
         * @param minEpoch the (inclusive) minimum epoch value for matched subplans
         */
        private final int minEpoch;

        public Matcher(int minEpoch) {
            this.minEpoch = minEpoch;
        }

        public List<SubplanMatch> match(PhysicalPlan plan) {
            new PlanTraversal(true, false)
                    .withCallback(this::attemptMatchFrom)
                    .traverse(plan.getSinks());
            // TODO: Traverse subplans and alternatives correctly!
            return this.matches;
        }

        /**
         * Try to match the given operator pattern..
         *
         * @param operator the operator that should be matched with the operator pattern
         */
        private void attemptMatchFrom(Operator operator, InputSlot<?> fromInputSlot, OutputSlot<?> fromOutputSlot) {
            if (fromInputSlot != null) {
                throw new IllegalStateException("Cannot handle downstream traversals.");
            }

            // Try to make a match starting from the currently visited operator.
            final SubplanMatch subplanMatch = new SubplanMatch(SubplanPattern.this);
            match(SubplanPattern.this.outputPattern, operator, fromOutputSlot, subplanMatch);
        }

        /**
         * Recursively match the given operator pattern and operator including their input operators.
         */
        private void match(OperatorPattern pattern,
                           Operator operator,
                           OutputSlot<?> trackedOutputSlot,
                           SubplanMatch subplanMatch) {

            if (pattern.getNumInputs() > 1) {
                throw new RuntimeException("Cannot match pattern: Operator with more than one input not supported, yet.");
            }

            if (operator instanceof Subplan) {
                if (trackedOutputSlot == null) {
                    match(pattern, ((Subplan) operator).enter(), trackedOutputSlot, subplanMatch);
                } else {
                    final OutputSlot<?> innerOutputSlot = ((Subplan) operator).enter(trackedOutputSlot);
                    match(pattern, innerOutputSlot.getOwner(), innerOutputSlot, subplanMatch);
                }

            } else if (operator instanceof OperatorAlternative) {
                for (OperatorAlternative.Alternative alternative : ((OperatorAlternative) operator).getAlternatives()) {
                    SubplanMatch subplanMatchCopy = new SubplanMatch(subplanMatch);
                    if (trackedOutputSlot == null) {
                        match(pattern, alternative.enter(), trackedOutputSlot, subplanMatchCopy);
                    } else {
                        final OutputSlot<?> innerOutputSlot = alternative.enter(trackedOutputSlot);
                        match(pattern, innerOutputSlot.getOwner(), innerOutputSlot, subplanMatchCopy);
                    }
                }

            } else if (operator instanceof OperatorAlternative.Alternative) {
                throw new IllegalStateException("Should not match against " +
                        OperatorAlternative.Alternative.class.getSimpleName());

            } else {
                // Try to match the co-iterated operator (pattern).
                final OperatorMatch operatorMatch = pattern.match(operator);
                if (operatorMatch == null) {
                    // If match was not successful, abort. NB: This might change if we have, like, real graph patterns.
                    return;
                }

                subplanMatch.addOperatorMatch(operatorMatch);

                // Now we need to try to match all the input operator patterns.
                boolean isTerminalOperator = true;
                for (int inputIndex = 0; inputIndex < operator.getNumInputs(); inputIndex++) {
                    final OperatorPattern inputOperatorPattern = (OperatorPattern) pattern.getInputOperator(inputIndex);
                    if (inputOperatorPattern == null) {
                        continue;
                    }
                    isTerminalOperator = false;
                    final InputSlot<?> outerInputSlot = operator.getOutermostInputSlot(operator.getInput(inputIndex));
                    final OutputSlot<?> occupant = outerInputSlot.getOccupant();
                    if (occupant != null) {
                        match(inputOperatorPattern, occupant.getOwner(), occupant, subplanMatch);
                    }

                }

                if (isTerminalOperator && subplanMatch.getMaximumEpoch() >= this.minEpoch) {
                    this.matches.add(subplanMatch);
                }
            }
        }
    }
}
