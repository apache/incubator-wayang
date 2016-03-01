package org.qcri.rheem.core.optimizer;

import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.plan.rheemplan.*;
import org.qcri.rheem.core.platform.ExecutionProfile;

import java.util.*;
import java.util.stream.Stream;

/**
 * Manages contextual information required during the optimization of a {@link RheemPlan}.
 * <p>A single {@link Operator} can have multiple contexts in a {@link RheemPlan} - namely if it appears in a loop.
 * We manage these contexts in a hierarchical fashion.</p>
 */
public class OptimizationContext {

    /**
     * The instance in that this instance is nested - or {@code null} if it is top-level.
     */
    private final OptimizationContext parent;

    /**
     * The iteration number of this instance within its {@link #parent} (starting from {@code 0}) - or {@code -1}
     * if there is no {@link #parent}.
     */
    private final int iterationNumber;

    /**
     * {@link OperatorContext}s of one-time {@link Operator}s (i.e., that are not nested in a loop).
     */
    private final Map<Operator, OperatorContext> operatorContexts = new HashMap<>();

    /**
     * {@link OptimizationContext}s (one per assumed iteration) of one-time {@link LoopSubplan}s (i.e., that are not
     * nested in a loop themselves).
     */
    private final Map<LoopSubplan, List<OptimizationContext>> nestedLoopContexts = new HashMap<>();

    /**
     * Create a new instance.
     *
     * @param rheemPlan that the new instance should describe; loops should already be isolated
     */
    public OptimizationContext(RheemPlan rheemPlan) {
        this(null, -1);
        PlanTraversal.upstream()
                .withCallback(this::addOneTimeOperator)
                .traverse(rheemPlan.getSinks());
    }

    /**
     * Creates a new instance. Useful for testing.
     *
     * @param operator the single {@link Operator} of this instance
     */
    public OptimizationContext(Operator operator) {
        this(null, -1);
        this.addOneTimeOperator(operator);
    }

    /**
     * Creates a new (nested) instance for the given {@code loop}.
     */
    private OptimizationContext(LoopSubplan loop, OptimizationContext parent, int iterationNumber) {
        this(parent, iterationNumber);
        this.addOneTimeOperators(loop);
    }

    /**
     * Base constructor.
     */
    private OptimizationContext(OptimizationContext parent, int iterationNumber) {
        this.parent = parent;
        this.iterationNumber = iterationNumber;
    }

    /**
     * Add {@link OperatorContext}s for the {@code operator} that is executed once within this instance. Also
     * add its encased {@link Operator}s.
     * Potentially invoke {@link #addOneTimeLoop(LoopSubplan)} as well.
     */
    private void addOneTimeOperator(Operator operator) {
        this.operatorContexts.put(operator, new OperatorContext(operator));
        if (!operator.isElementary()) {
            if (operator.isLoopSubplan()) {
                this.addOneTimeLoop((LoopSubplan) operator);
            } else if (operator.isAlternative()) {
                final OperatorAlternative operatorAlternative = (OperatorAlternative) operator;
                operatorAlternative.getAlternatives().forEach(this::addOneTimeOperators);
            } else {
                assert operator.isSubplan();
                this.addOneTimeOperators((Subplan) operator);
            }
        }
    }

    /**
     * Add {@link OperatorContext}s for all the contained {@link Operator}s of the {@code container}.
     */
    private void addOneTimeOperators(OperatorContainer container) {
        final CompositeOperator compositeOperator = container.toOperator();
        final Stream<Operator> innerOutputOperatorStream = compositeOperator.isSink() ?
                Stream.of(container.getSink()) :
                Arrays.stream(compositeOperator.getAllOutputs())
                        .map(container::traceOutput)
                        .filter(Objects::nonNull)
                        .map(Slot::getOwner);
        PlanTraversal.upstream()
                .withCallback(this::addOneTimeOperator)
                .traverse(innerOutputOperatorStream);
    }

    /**
     * Add {@link OptimizationContext}s for the {@code loop} that is executed once within this instance.
     */
    private void addOneTimeLoop(LoopSubplan loop) {
        loop.getNumExpectedIterations();
        List<OptimizationContext> iterationContexts = new ArrayList<>(loop.getNumExpectedIterations());
        for (int iterationNumber = 0; iterationNumber < loop.getNumExpectedIterations(); iterationNumber++) {
            iterationContexts.add(new OptimizationContext(loop, this, iterationNumber));
        }
        this.nestedLoopContexts.put(loop, iterationContexts);
    }

    /**
     * Return the {@link OperatorContext} of the {@code operator}.
     *
     * @param operator a one-time {@link Operator} (i.e., not in a nested loop)
     * @return the {@link OperatorContext} for the {@link Operator} or {@code null} if none
     */
    public OperatorContext getOperatorContext(Operator operator) {
        return this.operatorContexts.get(operator);
    }

    /**
     * Retrieve the instances for the iterations of a {@code loopSubplan}.
     *
     * @return a {@link List} of instances; ordered by their iteration number (see {@link #getIterationNumber()})
     */
    public List<OptimizationContext> getNestedLoopContexts(LoopSubplan loopSubplan) {
        return this.nestedLoopContexts.get(loopSubplan);
    }

    /**
     * @return if this instance describes an iteration within a {@link LoopSubplan}, return the number of that iteration
     * (starting at {@code 0}); otherwise {@code -1}
     */
    public int getIterationNumber() {
        return this.iterationNumber;
    }

    /**
     * @return if this instance describes an iteration within a {@link LoopSubplan}, return the instance in which
     * this instance is nested
     */
    public OptimizationContext getParent() {
        return this.parent;
    }

    /**
     * Represents a single optimization context of an {@link Operator}. This can be thought of as a single, virtual
     * execution of the {@link Operator}.
     */
    public class OperatorContext {

        /**
         * The {@link Operator} that is being decorated with this instance.
         */
        private final Operator operator;

        /**
         * {@link CardinalityEstimate}s that align with the {@link #operator}s {@link InputSlot}s and
         * {@link OutputSlot}s.
         */
        private final CardinalityEstimate[] inputCardinalities, outputCardinalities;

        private final boolean[] inputCardinalityMarkers, outputCardinalityMarkers;

        /**
         * {@link ExecutionProfile} of the {@link Operator}.
         */
        private ExecutionProfile executionProfile;

        /**
         * Creates a new instance.
         */
        public OperatorContext(Operator operator) {
            this.operator = operator;
            this.inputCardinalities = new CardinalityEstimate[this.operator.getNumInputs()];
            this.inputCardinalityMarkers = new boolean[this.inputCardinalities.length];
            this.outputCardinalities = new CardinalityEstimate[this.operator.getNumOutputs()];
            this.outputCardinalityMarkers = new boolean[this.outputCardinalities.length];
        }

        public Operator getOperator() {
            return this.operator;
        }

        public CardinalityEstimate getOutputCardinality(int outputIndex) {
            return this.outputCardinalities[outputIndex];
        }

        public CardinalityEstimate getInputCardinality(int inputIndex) {
            return this.inputCardinalities[inputIndex];
        }

        public boolean isInputMarked(int inputIndex) {
            return this.inputCardinalityMarkers[inputIndex];
        }

        public boolean isOutputMarked(int outputIndex) {
            return this.outputCardinalityMarkers[outputIndex];
        }

        /**
         * Resets the marks for all {@link InputSlot}s and {@link OutputSlot}s.
         */
        public void clearMarks() {
            Arrays.fill(this.inputCardinalityMarkers, false);
            Arrays.fill(this.outputCardinalityMarkers, false);
        }

        public CardinalityEstimate[] getInputCardinalities() {
            return this.inputCardinalities;
        }

        public CardinalityEstimate[] getOutputCardinalities() {
            return this.outputCardinalities;
        }

        /**
         * Sets the {@link CardinalityEstimate} for a certain {@link OutputSlot}. If the {@link CardinalityEstimate}
         * changes, the {@link OutputSlot} is marked.
         */
        public void setInputCardinality(int inputIndex, CardinalityEstimate cardinality) {
            this.inputCardinalityMarkers[inputIndex] |= !Objects.equals(this.inputCardinalities[inputIndex], cardinality);
            this.inputCardinalities[inputIndex] = cardinality;
        }

        /**
         * Sets the {@link CardinalityEstimate} for a certain {@link InputSlot}. If the {@link CardinalityEstimate}
         * changes, the {@link InputSlot} is marked.
         */
        public void setOutputCardinality(int outputIndex, CardinalityEstimate cardinality) {
            this.outputCardinalityMarkers[outputIndex] |= !Objects.equals(this.outputCardinalities[outputIndex], cardinality);
            this.outputCardinalities[outputIndex] = cardinality;
        }

        /**
         * Push forward all marked {@link CardinalityEstimate}s.
         *
         * @see Operator#propagateOutputCardinality(int, OperatorContext)
         */
        public void pushCardinalitiesForward() {
            for (int outputIndex = 0; outputIndex < this.outputCardinalities.length; outputIndex++) {
                if (!this.outputCardinalityMarkers[outputIndex]) continue;
                this.operator.propagateOutputCardinality(outputIndex, this);
            }
        }

        /**
         * @return the {@link OptimizationContext} in which this instance resides
         */
        public OptimizationContext getOptimizationContext() {
            return OptimizationContext.this;
        }
    }


}
