package org.qcri.rheem.core.optimizer;

import org.json.JSONArray;
import org.json.JSONObject;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.channels.ChannelConversionGraph;
import org.qcri.rheem.core.optimizer.costs.LoadProfile;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileToTimeConverter;
import org.qcri.rheem.core.optimizer.costs.TimeEstimate;
import org.qcri.rheem.core.optimizer.enumeration.PlanEnumerationPruningStrategy;
import org.qcri.rheem.core.plan.rheemplan.*;
import org.qcri.rheem.core.platform.ExecutionState;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.JsonSerializable;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.qcri.rheem.core.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Manages contextual information required during the optimization of a {@link RheemPlan}.
 * <p>A single {@link Operator} can have multiple contexts in a {@link RheemPlan} - namely if it appears in a loop.
 * We manage these contexts in a hierarchical fashion.</p>
 */
public abstract class OptimizationContext {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * The instance in that this instance is nested - or {@code null} if it is top-level.
     */
    protected final LoopContext hostLoopContext;

    /**
     * The iteration number of this instance within its {@link #hostLoopContext} (starting from {@code 0}) - or {@code -1}
     * if there is no {@link #hostLoopContext}.
     */
    private final int iterationNumber;

    /**
     * Forked {@link OptimizationContext}s can have a base.
     */
    private final OptimizationContext base;

    /**
     * {@link Configuration} that is used to create estimates here.
     */
    private final Configuration configuration;

    /**
     * {@link ChannelConversionGraph} used for the optimization.
     */
    private final ChannelConversionGraph channelConversionGraph;

    /**
     * {@link PlanEnumerationPruningStrategy}s to be used during optimization (in the given order).
     */
    private final List<PlanEnumerationPruningStrategy> pruningStrategies;

    /**
     * Create a new, plain instance.
     */
    public OptimizationContext(Configuration configuration) {
        this(configuration,
                null,
                null,
                -1,
                new ChannelConversionGraph(configuration),
                initializePruningStrategies(configuration));
    }

    /**
     * Creates a new instance. Useful for testing.
     *
     * @param operator the single {@link Operator} of this instance
     */
    public OptimizationContext(Operator operator, Configuration configuration) {
        this(configuration, null, null, -1, new ChannelConversionGraph(configuration), initializePruningStrategies(configuration));
        this.addOneTimeOperator(operator);
    }

    /**
     * Creates a new (nested) instance for the given {@code loop}.
     */
    private OptimizationContext(LoopSubplan loop, LoopContext hostLoopContext, int iterationNumber, Configuration configuration) {
        this(configuration, null, hostLoopContext, iterationNumber,
                hostLoopContext.getOptimizationContext().getChannelConversionGraph(),
                hostLoopContext.getOptimizationContext().getPruningStrategies());
        this.addOneTimeOperators(loop);
    }

    /**
     * Base constructor.
     */
    protected OptimizationContext(Configuration configuration, OptimizationContext base, LoopContext hostLoopContext,
                                  int iterationNumber, ChannelConversionGraph channelConversionGraph,
                                  List<PlanEnumerationPruningStrategy> pruningStrategies) {
        this.configuration = configuration;
        this.base = base;
        this.hostLoopContext = hostLoopContext;
        this.iterationNumber = iterationNumber;
        this.channelConversionGraph = channelConversionGraph;
        this.pruningStrategies = pruningStrategies;
    }

    /**
     * Initializes the {@link PlanEnumerationPruningStrategy}s from the {@link Configuration}.
     *
     * @param configuration defines the {@link PlanEnumerationPruningStrategy}s
     * @return a {@link List} of configured {@link PlanEnumerationPruningStrategy}s
     */
    private static List<PlanEnumerationPruningStrategy> initializePruningStrategies(Configuration configuration) {
        return configuration.getPruningStrategyClassProvider().provideAll().stream()
                .map(strategyClass -> OptimizationUtils.createPruningStrategy(strategyClass, configuration))
                .collect(Collectors.toList());
    }

    /**
     * Add {@link OperatorContext}s for the {@code operator} that is executed once within this instance. Also
     * add its encased {@link Operator}s.
     * Potentially invoke {@link #addOneTimeLoop(OperatorContext)} as well.
     */
    public abstract OperatorContext addOneTimeOperator(Operator operator);

    /**
     * Add {@link OperatorContext}s for all the contained {@link Operator}s of the {@code container}.
     */
    public void addOneTimeOperators(OperatorContainer container) {
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
    public abstract void addOneTimeLoop(OperatorContext operatorContext);

    /**
     * Return the {@link OperatorContext} of the {@code operator}.
     *
     * @param operator a one-time {@link Operator} (i.e., not in a nested loop)
     * @return the {@link OperatorContext} for the {@link Operator} or {@code null} if none
     */
    public abstract OperatorContext getOperatorContext(Operator operator);

    /**
     * Retrieve the {@link LoopContext} for the {@code loopSubplan}.
     */
    public abstract LoopContext getNestedLoopContext(LoopSubplan loopSubplan);

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
        assert this.hostLoopContext != null : String.format("%s is the last iteration.", this);
        assert !this.isFinalIteration();
        return this.hostLoopContext.getIterationContexts().get(this.iterationNumber + 1);
    }

    /**
     * Calls {@link OperatorContext#clearMarks()} for all nested {@link OperatorContext}s.
     */
    public abstract void clearMarks();

    public Configuration getConfiguration() {
        return this.configuration;
    }

    /**
     * @return the {@link OperatorContext}s of this instance (exclusive of any base instance)
     */
    public abstract Map<Operator, OperatorContext> getLocalOperatorContexts();

    /**
     * @return whether there is {@link TimeEstimate} for each {@link ExecutionOperator}
     */
    public abstract boolean isTimeEstimatesComplete();

    public ChannelConversionGraph getChannelConversionGraph() {
        return this.channelConversionGraph;
    }

    public OptimizationContext getBase() {
        return this.base;
    }

    public abstract void mergeToBase();

    public List<PlanEnumerationPruningStrategy> getPruningStrategies() {
        return this.pruningStrategies;
    }

    /**
     * Get the top-level parent containing this instance.
     *
     * @return the top-level parent, which can also be this instance
     */
    public OptimizationContext getRootParent() {
        OptimizationContext optimizationContext = this;
        while (true) {
            final OptimizationContext parent = optimizationContext.getParent();
            if (parent == null) return optimizationContext;
            optimizationContext = parent;
        }
    }

    /**
     * Get the {@link DefaultOptimizationContext}s represented by this instance.
     *
     * @return a {@link Collection} of said {@link DefaultOptimizationContext}s
     */
    public abstract Collection<DefaultOptimizationContext> getDefaultOptimizationContexts();

    @SuppressWarnings("unchecked")
    public OperatorContext addOperatorContextFromJson(JSONObject json) {
        Operator operator;
        try {
            String operatorClassName = json.getString("operator");
            Class<Operator> operatorClass = (Class<Operator>) Class.forName(operatorClassName);
            operator = ReflectionUtils.instantiateSomehow(
                    operatorClass,
                    new Tuple<>(DataSetType.class, DataSetType::none)
            );
        } catch (Throwable t) {
            throw new RheemException(String.format("Could not instantiate %s.", json.getString("operator")), t);
        }

        final JSONArray input = json.getJSONArray("input");
        for (int inputIndex = 0; inputIndex < input.length(); inputIndex++) {
            final JSONObject jsonInput = input.getJSONObject(inputIndex);
            if (jsonInput.getBoolean("isBroadcast")) {
                operator.addBroadcastInput(new InputSlot<>(
                        jsonInput.getString("name"), operator, true, DataSetType.none()
                ));
            }
        }

        final OperatorContext operatorContext = new OperatorContext(operator);
        for (int inputIndex = 0; inputIndex < input.length(); inputIndex++) {
            final JSONObject jsonInput = input.getJSONObject(inputIndex);
            operatorContext.setInputCardinality(inputIndex, CardinalityEstimate.fromJson(jsonInput));
        }

        JSONArray output = json.getJSONArray("output");
        for (int outputIndex = 0; outputIndex < output.length(); outputIndex++) {
            final JSONObject jsonOutput = output.getJSONObject(outputIndex);
            operatorContext.setOutputCardinality(outputIndex, CardinalityEstimate.fromJson(jsonOutput));
        }

        return operatorContext;
    }

    /**
     * Represents a single optimization context of an {@link Operator}. This can be thought of as a single, virtual
     * execution of the {@link Operator}.
     */
    public class OperatorContext implements JsonSerializable {

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
         * {@link TimeEstimate} for the {@link ExecutionState}.
         */
        private TimeEstimate timeEstimate;

        /**
         * Reflects the number of executions of the {@link #operator}. This, e.g., relevant in {@link LoopContext}s.
         */
        private int numExecutions = 1;

        /**
         * Creates a new instance.
         */
        protected OperatorContext(Operator operator) {
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
            if (OptimizationContext.this.logger.isDebugEnabled()) {
                OptimizationContext.this.logger.debug(
                        "Setting cardinality of {} to {}.", this.operator.getInput(inputIndex), cardinality
                );
            }
        }

        /**
         * Sets the {@link CardinalityEstimate} for a certain {@link InputSlot}. If the {@link CardinalityEstimate}
         * changes, the {@link InputSlot} is marked.
         */
        public void setOutputCardinality(int outputIndex, CardinalityEstimate cardinality) {
            this.outputCardinalityMarkers[outputIndex] |= !Objects.equals(this.outputCardinalities[outputIndex], cardinality);
            this.outputCardinalities[outputIndex] = cardinality;
            if (OptimizationContext.this.logger.isDebugEnabled()) {
                OptimizationContext.this.logger.debug(
                        "Setting cardinality of {} to {}.", this.operator.getOutput(outputIndex), cardinality
                );
            }
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
         */
        public void updateTimeEstimate() {
            this.updateTimeEstimate(this.getOptimizationContext().getConfiguration());
        }

        /**
         * Update the {@link LoadProfile} and {@link TimeEstimate} of this instance.
         *
         * @param configuration provides the necessary functions
         */
        private void updateTimeEstimate(Configuration configuration) {
            if (!this.operator.isExecutionOperator()) return;

            final ExecutionOperator executionOperator = (ExecutionOperator) this.operator;
            final LoadProfileEstimator loadProfileEstimator = configuration
                    .getOperatorLoadProfileEstimatorProvider()
                    .provideFor(executionOperator);
            try {
                this.loadProfile = loadProfileEstimator.estimate(this);
            } catch (Exception e) {
                throw new RheemException(String.format("Load profile estimation for %s failed.", this.operator), e);
            }

            final Platform platform = executionOperator.getPlatform();
            final LoadProfileToTimeConverter timeConverter = configuration.getLoadProfileToTimeConverterProvider().provideFor(platform);
            this.timeEstimate = TimeEstimate.MINIMUM.plus(timeConverter.convert(this.loadProfile));
            if (OptimizationContext.this.logger.isDebugEnabled()) {
                OptimizationContext.this.logger.debug(
                        "Setting time estimate of {} to {}.", this.operator, this.timeEstimate
                );
            }
        }

        public void increaseBy(OperatorContext that) {
            assert this.operator.equals(that.operator);
            this.addTo(this.inputCardinalities, that.inputCardinalities);
            this.addTo(this.inputCardinalityMarkers, that.inputCardinalityMarkers);
            this.addTo(this.outputCardinalities, that.outputCardinalities);
            this.addTo(this.outputCardinalityMarkers, that.outputCardinalityMarkers);
            this.timeEstimate = this.timeEstimate == null ?
                    that.timeEstimate :
                    that.timeEstimate == null ?
                            this.timeEstimate :
                            this.timeEstimate.plus(that.timeEstimate);
            this.numExecutions++;
        }

        private void addTo(CardinalityEstimate[] aggregate, CardinalityEstimate[] delta) {
            assert aggregate.length == delta.length;
            for (int i = 0; i < aggregate.length; i++) {
                CardinalityEstimate aggregateCardinality = aggregate[i];
                CardinalityEstimate deltaCardinality = delta[i];
                if (aggregateCardinality == null) {
                    aggregate[i] = deltaCardinality;
                } else if (deltaCardinality != null) {
                    aggregate[i] = aggregateCardinality.plus(deltaCardinality);
                }
            }
        }

        private void addTo(boolean[] aggregate, boolean[] delta) {
            assert aggregate.length == delta.length;
            for (int i = 0; i < aggregate.length; i++) {
                aggregate[i] |= delta[i];
            }
        }

        public void setNumExecutions(int numExecutions) {
            this.numExecutions = numExecutions;
        }

        public int getNumExecutions() {
            return this.numExecutions;
        }

        public TimeEstimate getTimeEstimate() {
            return this.timeEstimate;
        }

        @Override
        public String toString() {
            return String.format("%s[%s]", this.getClass().getSimpleName(), this.getOperator());
        }

        @Override
        public JSONObject toJson() {
            JSONObject jsonThis = new JSONObject();
            jsonThis.put("operator", this.operator.getClass().getSimpleName());
            jsonThis.put("executions", this.numExecutions);
            JSONArray jsonInputCardinalities = new JSONArray();
            for (int inputIndex = 0; inputIndex < inputCardinalities.length; inputIndex++) {
                InputSlot<?> input = this.operator.getInput(inputIndex);
                CardinalityEstimate inputCardinality = inputCardinalities[inputIndex];
                if (inputCardinality != null) {
                    jsonInputCardinalities.put(this.convertToJson(input, inputCardinality));
                }
            }
            jsonThis.put("input", inputCardinalities);
            JSONArray jsonOutputCardinalities = new JSONArray();
            for (int outputIndex = 0; outputIndex < outputCardinalities.length; outputIndex++) {
                OutputSlot<?> output = this.operator.getOutput(outputIndex);
                CardinalityEstimate outputCardinality = outputCardinalities[outputIndex];
                if (outputCardinality != null) {
                    jsonOutputCardinalities.put(this.convertToJson(output, outputCardinality));
                }
            }
            jsonThis.put("output", inputCardinalities);

            return jsonThis;
        }

        private JSONObject convertToJson(Slot<?> slot, CardinalityEstimate cardinality) {
            JSONObject json = new JSONObject();
            json.put("name", slot.getName());
            json.put("index", slot.getIndex());
            if (slot instanceof InputSlot<?>) {
                json.put("isBroadcast", ((InputSlot<?>) slot).isBroadcast());
            }
            json.put("lowerBound", cardinality.getLowerEstimate());
            json.put("upperBound", cardinality.getUpperEstimate());
            json.put("confidence", cardinality.getCorrectnessProbability());
            return json;
        }
    }

    /**
     * Maintains {@link OptimizationContext}s for the iterations of a {@link LoopSubplan}.
     */
    public class LoopContext {

        private final OperatorContext loopSubplanContext;

        private final List<OptimizationContext> iterationContexts;

        protected LoopContext(OperatorContext loopSubplanContext) {
            assert loopSubplanContext.getOptimizationContext() == OptimizationContext.this;
            assert loopSubplanContext.getOperator() instanceof LoopSubplan;

            this.loopSubplanContext = loopSubplanContext;

            LoopSubplan loop = (LoopSubplan) loopSubplanContext.getOperator();
            final int numIterationContexts = loop.getNumExpectedIterations() + 1;
            this.iterationContexts = new ArrayList<>(numIterationContexts);
            for (int iterationNumber = 0; iterationNumber < numIterationContexts; iterationNumber++) {
                this.iterationContexts.add(new DefaultOptimizationContext(loop, this, iterationNumber, OptimizationContext.this.configuration));
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

        public OptimizationContext getIterationContext(int iteration) {
            return this.iterationContexts.get(iteration);
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

        public LoopSubplan getLoop() {
            return (LoopSubplan) this.loopSubplanContext.getOperator();
        }

        public OptimizationContext createAggregateContext() {
            return this.createAggregateContext(0, this.iterationContexts.size());
        }

        public OptimizationContext createAggregateContext(int fromIteration, int toIteration) {
            return new AggregateOptimizationContext(
                    this,
                    fromIteration == 0 && toIteration == this.iterationContexts.size() ?
                            this.iterationContexts :
                            this.iterationContexts.subList(fromIteration, toIteration)
            );
        }
    }


}
