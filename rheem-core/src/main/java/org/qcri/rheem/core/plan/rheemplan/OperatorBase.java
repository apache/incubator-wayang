package org.qcri.rheem.core.plan.rheemplan;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.function.FlatMapDescriptor;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.function.ReduceDescriptor;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.Tuple;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Helper class for the implementation of the {@link Operator} interface.
 */
public abstract class OperatorBase implements Operator {

    public static final List<Tuple<Class<?>, Supplier<?>>> STANDARD_OPERATOR_ARGS = Arrays.asList(
            new Tuple<>(DataSetType.class, DataSetType::none),
            new Tuple<>(Class.class, () -> Object.class),
            new Tuple<>(TransformationDescriptor.class, () -> new TransformationDescriptor<>(o -> o, Object.class, Object.class)),
            new Tuple<>(FlatMapDescriptor.class, () -> new FlatMapDescriptor<>(o -> Collections.emptyList(), Object.class, Object.class)),
            new Tuple<>(PredicateDescriptor.class, () -> new PredicateDescriptor<>(o -> true, Object.class)),
            new Tuple<>(ReduceDescriptor.class, () -> new ReduceDescriptor<>((a, b) -> a, Object.class)),
            new Tuple<>(FunctionDescriptor.SerializableFunction.class, () -> (FunctionDescriptor.SerializableFunction) o -> o),
            new Tuple<>(FunctionDescriptor.SerializableBinaryOperator.class, () -> (FunctionDescriptor.SerializableBinaryOperator) (a, b) -> a),
            new Tuple<>(Object[].class, () -> new Object[0]),
            new Tuple<>(String[].class, () -> new String[0])
    );

    private final boolean isSupportingBroadcastInputs;

    private OperatorContainer container;

    /**
     * Tells whether this instance is auxiliary, i.e., it support some non-auxiliary operators.
     */
    private boolean isAuxiliary = false;

    private int epoch = FIRST_EPOCH;

    protected InputSlot<?>[] inputSlots;

    protected final OutputSlot<?>[] outputSlots;

    private final Set<Platform> targetPlatforms = new HashSet<>(0);

    private ExecutionOperator original;

    /**
     * Optional {@link CardinalityEstimator}s for this instance.
     */
    private CardinalityEstimator[] cardinalityEstimators;

    /**
     * Optional name. Helpful for debugging.
     */
    private String name;

    public OperatorBase(InputSlot<?>[] inputSlots, OutputSlot<?>[] outputSlots, boolean isSupportingBroadcastInputs) {
        this.container = null;
        this.isSupportingBroadcastInputs = isSupportingBroadcastInputs;
        this.inputSlots = inputSlots;
        this.outputSlots = outputSlots;
        this.cardinalityEstimators = new CardinalityEstimator[this.outputSlots.length];
    }

    public OperatorBase(int numInputSlots, int numOutputSlots, boolean isSupportingBroadcastInputs) {
        this(new InputSlot[numInputSlots], new OutputSlot[numOutputSlots], isSupportingBroadcastInputs);
    }

    /**
     * Creates a plain copy of the given {@link OperatorBase}, including
     * <ul>
     * <li>the number of regular {@link InputSlot}s (not the actual {@link InputSlot}s, though)</li>
     * <li>the number of {@link OutputSlot}s (not the actual {@link OutputSlot}s, though)</li>
     * <li>whether broadcasts are supported</li>
     * <li>any specific {@link CardinalityEstimator}s</li>
     * </ul>
     *
     * @param that the {@link OperatorBase} to be copied
     */
    protected OperatorBase(OperatorBase that) {
        this(that.getNumRegularInputs(), that.getNumOutputs(), that.isSupportingBroadcastInputs());
        System.arraycopy(that.cardinalityEstimators, 0, this.cardinalityEstimators, 0, this.getNumOutputs());
    }

    @Override
    public InputSlot<?>[] getAllInputs() {
        return this.inputSlots;
    }

    @Override
    public OutputSlot<?>[] getAllOutputs() {
        return this.outputSlots;
    }

    @Override
    public boolean isSupportingBroadcastInputs() {
        return this.isSupportingBroadcastInputs;
    }

    @Override
    public int addBroadcastInput(InputSlot<?> broadcastInput) {
        Validate.isTrue(this.isSupportingBroadcastInputs(), "%s does not support broadcast inputs.", this);
        Validate.isTrue(
                Arrays.stream(this.getAllInputs()).noneMatch(input -> input.getName().equals(broadcastInput.getName())),
                "The name %s is already taken in %s.", broadcastInput.getName(), this
        );
        Validate.isTrue(broadcastInput.isBroadcast());
        final int oldNumInputSlots = this.getNumInputs();
        final InputSlot<?>[] newInputs = new InputSlot<?>[oldNumInputSlots + 1];
        System.arraycopy(this.getAllInputs(), 0, newInputs, 0, oldNumInputSlots);
        newInputs[oldNumInputSlots] = broadcastInput;
        this.inputSlots = newInputs;
        return oldNumInputSlots;
    }

    @Override
    public <Payload, Return> Return accept(TopDownPlanVisitor<Payload, Return> visitor, OutputSlot<?> outputSlot, Payload payload) {
        return null;
    }

    @Override
    public OperatorContainer getContainer() {
        return this.container;
    }

    @Override
    public void setContainer(OperatorContainer newContainer) {
        final OperatorContainer formerContainer = this.getContainer();
        this.container = newContainer;
        if (formerContainer != null) {
            formerContainer.noteReplaced(this, newContainer);
        }
    }

    @Override
    public int getEpoch() {
        return this.epoch;
    }

    @Override
    public void setEpoch(int epoch) {
        this.epoch = epoch;
    }

    /**
     * Convenience method to set the epoch.
     */
    public Operator at(int epoch) {
        this.setEpoch(epoch);
        return this;
    }

    @Override
    public String toString() {
        if (this.name != null) {
            return String.format("%s[%s]", this.getSimpleClassName(), this.name);
        }
        long numBroadcasts = Arrays.stream(this.getAllInputs()).filter(InputSlot::isBroadcast).count();
        return String.format("%s[%d%s->%d, id=%x]",
                this.getSimpleClassName(),
                this.getNumInputs() - numBroadcasts,
                numBroadcasts == 0 ? "" : "+" + numBroadcasts,
                this.getNumOutputs(),
//                this.getParent() == null ? "top-level" : "nested",
                this.hashCode());
    }

    protected String getSimpleClassName() {
        String className = this.getClass().getSimpleName();
        return className.replaceAll("Operator", "");
    }

    @Override
    public Set<Platform> getTargetPlatforms() {
        return this.targetPlatforms;
    }

    @Override
    public void addTargetPlatform(Platform platform) {
        this.targetPlatforms.add(platform);
    }

    @Override
    public void propagateOutputCardinality(int outputIndex,
                                           OptimizationContext.OperatorContext operatorContext,
                                           OptimizationContext targetContext) {
        assert operatorContext.getOperator() == this;

        // Identify the cardinality.
        final CardinalityEstimate cardinality = operatorContext.getOutputCardinality(outputIndex);
        final OutputSlot<?> localOutput = this.getOutput(outputIndex);

        // Propagate to InputSlots.
        for (final OutputSlot<?> outerOutput : this.getOutermostOutputSlots(localOutput)) {
            // Propagate to the InputSlots.
            for (InputSlot<?> inputSlot : outerOutput.getOccupiedSlots()) {
                // Find the adjacent OperatorContext corresponding to the inputSlot.
                final int inputIndex = inputSlot.getIndex();
                final Operator adjacentOperator = inputSlot.getOwner();
                final OptimizationContext.OperatorContext adjacentOperatorCtx = targetContext.getOperatorContext(adjacentOperator);
                assert adjacentOperatorCtx != null : String.format("Missing OperatorContext for %s.", adjacentOperator);

                // Update the adjacent OperatorContext.
                adjacentOperatorCtx.setInputCardinality(inputIndex, cardinality);
                adjacentOperator.propagateInputCardinality(inputIndex, adjacentOperatorCtx);
            }
        }


    }

    @Override
    public void propagateInputCardinality(int inputIndex, OptimizationContext.OperatorContext operatorContext) {
        // Nothing to do for elementary operators.
    }

    @Override
    public <T> Set<OutputSlot<T>> collectMappedOutputSlots(OutputSlot<T> output) {
        // Default implementation for elementary instances.
        assert this.isElementary();
        assert output.getOwner() == this;
        return Collections.singleton(output);
    }

    @Override
    public <T> Set<InputSlot<T>> collectMappedInputSlots(InputSlot<T> input) {
        // Default implementation for elementary instances.
        assert this.isElementary();
        assert input.getOwner() == this;
        return Collections.singleton(input);
    }

    /**
     * @see ExecutionOperator#copy()
     */
    public ExecutionOperator copy() {
        assert this.isExecutionOperator();
        ExecutionOperator copy = this.createCopy();
        ((OperatorBase) copy).original = this.getOriginal();
        return copy;
    }

    protected ExecutionOperator createCopy() {
        throw new RuntimeException("Not implemented.");
    }

    /**
     * @see ExecutionOperator#getOriginal()
     */
    public ExecutionOperator getOriginal() {
        assert this.isExecutionOperator();
        return this.original == null ? (ExecutionOperator) this : this.original;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Retrieve a {@link CardinalityEstimator} tied specifically to this instance. Applicable to
     * {@link ElementaryOperator}s only.
     *
     * @param outputIndex for the output described by the {@code cardinalityEstimator}
     * @return the {@link CardinalityEstimator} or {@code null} if none exists
     */
    public CardinalityEstimator getCardinalityEstimator(int outputIndex) {
        Validate.isAssignableFrom(ElementaryOperator.class, this.getClass());
        return this.cardinalityEstimators[outputIndex];
    }

    /**
     * Tie a specific {@link CardinalityEstimator} to this instance. Applicable to {@link ElementaryOperator}s
     * only.
     *
     * @param outputIndex          for the output described by the {@code cardinalityEstimator}
     * @param cardinalityEstimator the {@link CardinalityEstimator}
     */
    public void setCardinalityEstimator(int outputIndex, CardinalityEstimator cardinalityEstimator) {
        Validate.isAssignableFrom(ElementaryOperator.class, this.getClass());
        this.cardinalityEstimators[outputIndex] = cardinalityEstimator;
    }

    public boolean isAuxiliary() {
        return this.isAuxiliary;
    }

    public void setAuxiliary(boolean auxiliaryOperator) {
        this.isAuxiliary = auxiliaryOperator;
    }

    /**
     * Utility to de/serialize {@link Operator}s.
     */
    public static class GsonSerializer implements JsonSerializer<Operator>, JsonDeserializer<Operator> {

        @Override
        public JsonElement serialize(Operator src, Type typeOfSrc, JsonSerializationContext context) {
            if (src == null) {
                return JsonNull.INSTANCE;
            }
            final JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty("_class", src.getClass().getName());
            jsonObject.addProperty("name", src.getName());
            return jsonObject;
        }

        @Override
        public Operator deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            if (JsonNull.INSTANCE.equals(json)) return null;
            throw new UnsupportedOperationException("Deserializing operators is not yet supported.");
        }
    }

}
