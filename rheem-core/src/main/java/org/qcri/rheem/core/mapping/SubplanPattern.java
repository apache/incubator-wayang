package org.qcri.rheem.core.mapping;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.OperatorBase;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.plan.rheemplan.PlanTraversal;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.plan.rheemplan.TopDownPlanVisitor;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A subplan pattern describes a class of subplans in a {@link RheemPlan}.
 * <p><i>NB: Currently, only such patterns are tested and supported that form a chain of operators, i.e., no DAGs
 * are allowed and at most one input and one output operator.</i></p>
 */
public class SubplanPattern extends OperatorBase {

    /**
     * Start and end {@link OperatorPattern} of this instance.
     */
    private final OperatorPattern inputPattern, outputPattern;

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
     * Creates a new instance.
     */
    private SubplanPattern(OperatorPattern inputPattern, OperatorPattern outputPattern) {
        super(inputPattern.getAllInputs(), outputPattern.getAllOutputs(), false);
        this.inputPattern = inputPattern;
        this.outputPattern = outputPattern;
    }

    /**
     * Match this pattern against a plan.
     *
     * @param plan     the plan to match against
     * @param minEpoch the (inclusive) minimum epoch value for matched subplans
     * @return all matches
     */
    public List<SubplanMatch> match(RheemPlan plan, int minEpoch) {
        return new Matcher(minEpoch).match(plan);
    }

    public OperatorPattern getInputPattern() {
        return this.inputPattern;
    }

    public OperatorPattern getOutputPattern() {
        return this.outputPattern;
    }

    @Override
    public <Payload, Return> Return accept(TopDownPlanVisitor<Payload, Return> visitor, OutputSlot<?> outputSlot, Payload payload) {
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

        /**
         * Creates a new instance.
         *
         * @param minEpoch see {@link #minEpoch}
         */
        public Matcher(int minEpoch) {
            this.minEpoch = minEpoch;
        }

        /**
         * Run this instance over the given {@link RheemPlan}.
         *
         * @return a {@link List} of all {@link SubplanMatch}es established by the run
         */
        public List<SubplanMatch> match(RheemPlan plan) {
            // Start an attempt to match from each operator that is upstream-reachable from one of the RheemPlan sinks.
            PlanTraversal.upstream().traversingHierarchically()
                    .withCallback(this::attemptMatchFrom)
                    .traverse(plan.getSinks());
            return this.matches;
        }

        /**
         * Try to match the given operator pattern..
         *
         * @param operator the operator that should be matched with the operator pattern
         */
        private void attemptMatchFrom(Operator operator, InputSlot<?> fromInputSlot, OutputSlot<?> fromOutputSlot) {
            Validate.isTrue(fromInputSlot == null, "Cannot handle downstream traversals.");

            // Try to make a match starting from the currently visited operator.
            final SubplanMatch subplanMatch = new SubplanMatch(SubplanPattern.this);
            this.match(SubplanPattern.this.outputPattern, operator, fromOutputSlot, subplanMatch);
        }

        /**
         * Recursively match the given operator pattern and operator including their input operators.
         *
         * @param pattern           the next {@link OperatorPattern} to match with
         * @param operator          the {@link Operator} that should match with the {@code pattern}
         * @param trackedOutputSlot the {@link OutputSlot} of the {@link Operator} that we are coming from
         * @param subplanMatch      collects the {@link OperatorMatch}es on success
         */
        private void match(OperatorPattern pattern,
                           Operator operator,
                           OutputSlot<?> trackedOutputSlot,
                           SubplanMatch subplanMatch) {

            // Make sure that nobody expects more from this instance than it can handle.
            if (pattern.getNumInputs() > 1 &&
                    Arrays.stream(pattern.getAllInputs())
                            .map(InputSlot::getOccupant)
                            .filter(Objects::nonNull)
                            .count() > 1) {
                throw new RheemException("Cannot match pattern: Operator with more than one occupied input not supported, yet.");
            }


            // We expect a regular Operator here.
            // Try to match the co-iterated operator (pattern).
            assert operator.isElementary();
            final OperatorMatch operatorMatch = pattern.match(operator);
            if (operatorMatch == null) {
                // If match was not successful, abort. NB: This might change if we have, like, real graph patterns.
                return;
            }

            subplanMatch.addOperatorMatch(operatorMatch);

            // Now we need to go further upstream and try to match all the input OperatorPatterns.
            // NB: As of now, that should be exactly one (see top).
            boolean hasInputOperatorPatterns = false;
            for (int inputIndex = 0; inputIndex < pattern.getNumInputs(); inputIndex++) {
                final OutputSlot<?> patternOccupant = pattern.getEffectiveOccupant(pattern.getInput(inputIndex));
                if (patternOccupant == null) {
                    continue;
                }
                final OperatorPattern inputOperatorPattern = (OperatorPattern) patternOccupant.getOwner();

                hasInputOperatorPatterns = true;
                final InputSlot<?> outerInputSlot = operator.getOutermostInputSlot(operator.getInput(inputIndex));
                final OutputSlot<?> occupant = outerInputSlot.getOccupant();
                if (occupant != null) {
                    this.match(inputOperatorPattern, occupant.getOwner(), occupant, subplanMatch);
                }

            }

            if (!hasInputOperatorPatterns) {
                if (subplanMatch.getMaximumEpoch() >= this.minEpoch) {
                    this.matches.add(subplanMatch);
                }
            }
        }
    }
}
