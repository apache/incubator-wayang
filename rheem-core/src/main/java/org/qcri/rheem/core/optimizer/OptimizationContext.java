package org.qcri.rheem.core.optimizer;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.costs.LoadProfile;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileToTimeConverter;
import org.qcri.rheem.core.optimizer.costs.TimeEstimate;
import org.qcri.rheem.core.plan.rheemplan.*;
import org.qcri.rheem.core.platform.ExecutionProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Stream;

/**
 * Manages contextual information required during the optimization of a {@link RheemPlan}.
 * <p>A single {@link Operator} can have multiple contexts in a {@link RheemPlan} - namely if it appears in a loop.
 * We manage these contexts in a hierarchical fashion.</p>
 */
public class OptimizationContext {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * The instance in that this instance is nested - or {@code null} if it is top-level.
     */
    private final LoopContext hostLoopContext;

    /**
     * The iteration number of this instance within its {@link #hostLoopContext} (starting from {@code 0}) - or {@code -1}
     * if there is no {@link #hostLoopContext}.
     */
    private final int iterationNumber;

    /**
     * {@link OperatorContext}s of one-time {@link Operator}s (i.e., that are not nested in a loop).
     */
    private final Map<Operator, OperatorContext> operatorContexts = new HashMap<>();

    /**
     * {@link LoopContext}s of one-time {@link LoopSubplan}s (i.e., that are not
     * nested in a loop themselves).
     */
    private final Map<LoopSubplan, LoopContext> loopContexts = new HashMap<>();

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
    private OptimizationContext(LoopSubplan loop, LoopContext hostLoopContext, int iterationNumber) {
        this(hostLoopContext, iterationNumber);
        this.addOneTimeOperators(loop);
    }

    /**
     * Base constructor.
     */
    private OptimizationContext(LoopContext hostLoopContext, int iterationNumber) {
        this.hostLoopContext = hostLoopContext;
        this.iterationNumber = iterationNumber;
    }

    /**
     * Add {@link OperatorContext}s for the {@code operator} that is executed once within this instance. Also
     * add its encased {@link Operator}s.
     * Potentially invoke {@link #addOneTimeLoop(OperatorContext)} as well.
     */
    private void addOneTimeOperator(Operator operator) {
        final OperatorContext operatorContext = new OperatorContext(operator);
        this.operatorContexts.put(operator, operatorContext);
        if (!operator.isElementary()) {
            if (operator.isLoopSubplan()) {
                this.addOneTimeLoop(operatorContext);
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
    private void addOneTimeLoop(OperatorContext operatorContext) {
        this.loopContexts.put((LoopSubplan) operatorContext.getOperator(), new LoopContext(operatorContext));
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
     * Retrieve the {@link LoopContext} for the {@code loopSubplan}.
     */
    public LoopContext getNestedLoopContext(LoopSubplan loopSubplan) {
        return this.loopContexts.get(loopSubplan);
    }

    /**
     * @return if this instance describes an iteration within a {@link LoopSubplan}, return the number of that iteration
     * (starting at {@code 0}); otherwise {@code -1}
     */
    public int getIterationNumber() {
        return this.iterationNumber;
    }

    /**
     * @return whether this instance is the first iteration within a {@link LoopContext}; this instance must be embedded
     * in a {@link LoopContext}
     */
    public boolean isInitialIteration() {
        assert this.hostLoopContext != null : "Not within a LoopContext.";
        return this.iterationNumber == 0;
    }


    /**
     * @return whether this instance is the final iteration within a {@link LoopContext}; this instance must be embedded
     * in a {@link LoopContext}
     */
    public boolean isFinalIteration() {
        assert this.hostLoopContext != null;
        return this.iterationNumber == this.hostLoopContext.getIterationContexts().size() - 1;
    }

    /**
     * @return if this instance describes an iteration within a {@link LoopSubplan}, return the instance in which
     * this instance is nested
     */
    public OptimizationContext getParent() {
        return this.hostLoopContext == null ? null : this.hostLoopContext.getOptimizationContext();
    }

    public OptimizationContext getNextIterationContext() {
        assert this.hostLoopContext != null;
        assert !this.isFinalIteration();
        return this.hostLoopContext.getIterationContexts().get(this.iterationNumber + 1);
    }

    /**
     * Calls {@link OperatorContext#clearMarks()} for all nested {@link OperatorContext}s.
     */
    public void clearMarks() {
        this.operatorContexts.values().forEach(OperatorContext::clearMarks);
        this.loopContexts.values().stream()
                .flatMap(loopCtx -> loopCtx.getIterationContexts().stream())
                .forEach(OptimizationContext::clearMarks);
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

        /**
         * Used to mark changed {@link #inputCardinalities} and {@link #outputCardinalities}.
         */
        private final boolean[] inputCardinalityMarkers, outputCardinalityMarkers;

        /**
         * {@link LoadProfile} of the {@link Operator}.
         */
        private LoadProfile loadProfile;

        /**
         * {@link TimeEstimate} for the {@link ExecutionProfile}.
         */
        private TimeEstimate timeEstimate;

        /**
         * Creates a new instance.
         */
        private OperatorContext(Operator operator) {
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
         * Push forward all marked {@link CardinalityEstimate}s within the same {@link OptimizationContext}.
         *
         * @see Operator#propagateOutputCardinality(int, OperatorContext)
         */
        public void pushCardinalitiesForward() {
            OptimizationContext targetContext = this.getOptimizationContext();
            for (int outputIndex = 0; outputIndex < this.outputCardinalities.length; outputIndex++) {
                this.pushCardinalityForward(outputIndex, targetContext);
            }
        }

        /**
         * Pushes the {@link CardinalityEstimate} corresponding to the {@code outputIndex} forward to the
         * {@code targetContext} if it is marked.
         */
        public void pushCardinalityForward(int outputIndex, OptimizationContext targetContext) {
            if (!this.outputCardinalityMarkers[outputIndex]) return;
            this.operator.propagateOutputCardinality(outputIndex, this, targetContext);
        }

        /**
         * @return the {@link OptimizationContext} in which this instance resides
         */
        public OptimizationContext getOptimizationContext() {
            return OptimizationContext.this;
        }

        /**
         * Update the {@link LoadProfile} and {@link TimeEstimate} of this instance.
         *
         * @param configuration provides the necessary functions
         */
        public void updateTimeEstimate(Configuration configuration) {
            if (!this.operator.isExecutionOperator()) return;

            final Optional<LoadProfileEstimator> optionalLoadProfileEstimator = configuration
                    .getOperatorLoadProfileEstimatorProvider()
                    .optionallyProvideFor((ExecutionOperator) this.operator);
            if (!optionalLoadProfileEstimator.isPresent()) {
                OptimizationContext.this.logger.warn("No LoadProfileEstimator for {} configured.", this.operator);
                return;
            }
            final LoadProfileEstimator loadProfileEstimator = optionalLoadProfileEstimator.get();
            this.loadProfile = loadProfileEstimator.estimate(this);

            final Optional<LoadProfileToTimeConverter> optionalLoadProfileToTimeConverter =
                    configuration.getLoadProfileToTimeConverterProvider().optionallyProvide();
            if (!optionalLoadProfileEstimator.isPresent()) {
                OptimizationContext.this.logger.warn("No LoadProfileToTimeConverter for {} configured.");
                return;
            }
            final LoadProfileToTimeConverter timeConverter = optionalLoadProfileToTimeConverter.get();
            this.timeEstimate = timeConverter.convert(this.loadProfile);
        }

        public TimeEstimate getTimeEstimate() {
            return this.timeEstimate;
        }
    }

    /**
     * Maintains {@link OptimizationContext}s for the iterations of a {@link LoopSubplan}.
     */
    public class LoopContext {

        private final OperatorContext loopSubplanContext;

        private final List<OptimizationContext> iterationContexts;

        private LoopContext(OperatorContext loopSubplanContext) {
            assert loopSubplanContext.getOptimizationContext() == OptimizationContext.this;

            this.loopSubplanContext = loopSubplanContext;

            LoopSubplan loop = (LoopSubplan) loopSubplanContext.getOperator();
            final int numIterationContexts = loop.getNumExpectedIterations() + 1;
            this.iterationContexts = new ArrayList<>(numIterationContexts);
            for (int iterationNumber = 0; iterationNumber < numIterationContexts; iterationNumber++) {
                this.iterationContexts.add(new OptimizationContext(loop, this, iterationNumber));
            }
        }

        public OperatorContext getLoopSubplanContext() {
            return this.loopSubplanContext;
        }

        /**
         * Retrieves the iteration {@link OptimizationContext}s.
         * <p>
         * <p>Note that for {@code n} iterations, there are
         * {@code n+1} {@link OptimizationContext}s because the {@link LoopHeadOperator} is triggered {@code n+1} times.
         * The first {@code n} represent the iterations, the final represents the final state of the loop, in which
         * only the {@link LoopHeadOperator} is run the last time.</p>
         *
         * @return the {@link OptimizationContext} for each iteration; order by execution order
         */
        public List<OptimizationContext> getIterationContexts() {
            return this.iterationContexts;
        }

        /**
         * @return the {@link OptimizationContext} in that the {@link LoopSubplan} resides
         */
        public OptimizationContext getOptimizationContext() {
            return this.getLoopSubplanContext().getOptimizationContext();
        }

        public OptimizationContext getInitialIterationContext() {
            return this.iterationContexts.get(0);
        }

        public OptimizationContext getFinalIterationContext() {
            return this.iterationContexts.get(this.iterationContexts.size() - 1);
        }
    }


}
